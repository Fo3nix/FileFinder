import os
import sys
import time
import argparse
from multiprocessing import Pool, cpu_count
from pathlib import Path

# Third-party dependencies
# pip install sqlalchemy tqdm
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

# --- Configuration ---
DATABASE_NAME = 'filesystem_index.db'
# Use modern SQLAlchemy 2.0 syntax for Base
Base = declarative_base()


# --- SQLAlchemy Models ---

class Folder(Base):
    """Represents a directory in the filesystem."""
    __tablename__ = 'folders'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('folders.id'), nullable=True)
    name = Column(String, nullable=False)
    path = Column(String, unique=True, nullable=False)  # Store full path for uniqueness and easy access

    # Relationships
    parent = relationship("Folder", remote_side=[id])
    files = relationship("File", back_populates="folder")

    # Index on the name for search speed
    __table_args__ = (
        Index('idx_folder_name', 'name'),
    )

    def __repr__(self):
        return f"<Folder(name='{self.name}', path='{self.path}')>"


class File(Base):
    """Represents a file in the filesystem."""
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    # A file must belong to a folder, so nullable=False
    folder_id = Column(Integer, ForeignKey('folders.id'), nullable=False)
    name = Column(String, nullable=False)

    # Relationships
    folder = relationship("Folder", back_populates="files")

    # Index on the name for search speed
    __table_args__ = (
        Index('idx_file_name', 'name'),
    )

    def __repr__(self):
        return f"<File(name='{self.name}', folder_id='{self.folder_id}')>"


# --- Database and Setup Functions ---

def setup_db(database_url):
    """Initializes the database and returns the Session factory and Engine."""
    engine = create_engine(database_url)
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session, engine


def get_drive_paths(drive_letter):
    """
    Recursively collects all folder paths on a given drive.
    This runs single-threaded to generate the task list quickly.
    """
    drive_path = Path(f'{drive_letter}:\\')
    print(f"Phase 1: Collecting all directory paths under {drive_path}...")

    if not drive_path.exists():
        print(f"Error: Drive {drive_letter} does not exist or is inaccessible.")
        return []

    # Use os.walk for robust and fast directory traversal
    folder_paths = []

    # onerror=lambda e: None will skip directories the user can't access
    for root, dirs, files in os.walk(drive_path, onerror=lambda e: None):
        # Add the current root directory
        folder_paths.append(Path(root))
        # No need to iterate over dirs or files here, just collect roots

    print(f"Found {len(folder_paths)} directories.")
    return folder_paths


# --- Parallel Worker Function ---

def process_directory_files(task_tuple):
    """
    Worker function run in parallel: finds files in a single directory
    and returns a list of dictionaries.
    This function does NOT interact with the database.
    """
    directory_path_str, folder_id = task_tuple
    files_to_add = []

    try:
        # os.scandir is generally faster than Path.iterdir() for raw speed
        # Filter for files only, skipping directories
        for entry in os.scandir(directory_path_str):
            if entry.is_file():
                # Append a dictionary, which is picklable and safe for multiprocessing
                files_to_add.append({'folder_id': folder_id, 'name': entry.name})

        return files_to_add

    except Exception as e:
        # Optionally log the error, but suppress file system access errors
        # which are common (e.g., permissions denied)
        # print(f"Error processing {directory_path_str}: {e}", file=sys.stderr)
        return []


# --- Main Logic ---

def main(drive_letter):
    database_url = f'sqlite:///{DATABASE_NAME}'
    Session, engine = setup_db(database_url)

    print(f"Database setup complete: {DATABASE_NAME}")

    # Phase 1: Collect all directories (Single-threaded)
    all_dir_paths = get_drive_paths(drive_letter)
    if not all_dir_paths:
        print("No paths found. Exiting.")
        return

    # Phase 2: Insert all Folders (Single-threaded)
    print("\nPhase 2: Inserting all folders into the database...")
    session = Session()

    # Dictionary to map full path string to its new primary key ID
    path_to_id = {}

    # Sort paths by depth to ensure parents are created before children
    all_dir_paths.sort(key=lambda p: len(p.parts))

    start_time_phase2 = time.time()
    try:
        for path in tqdm(all_dir_paths, desc="Indexing Folders"):
            path_str = str(path)

            # Determine parent_id by looking up the parent's path string
            parent_id = None
            parent_path_str = str(path.parent)
            if path.parent and parent_path_str in path_to_id:
                parent_id = path_to_id[parent_path_str]

            folder = Folder(
                name=path.name or str(path),  # Use path if name is empty (like C:\)
                path=path_str,
                parent_id=parent_id
            )
            session.add(folder)

            # Flush session to get the new folder.id *before* committing
            # This makes the ID available for subsequent child folders
            session.flush()

            # Store the generated ID for child-lookups
            path_to_id[path_str] = folder.id

        # Commit all folders in one transaction at the end
        session.commit()

        end_time_phase2 = time.time()
        print(f"Successfully indexed {len(path_to_id)} folders in {end_time_phase2 - start_time_phase2:.2f}s.")

    except IntegrityError:
        session.rollback()
        print("Error: Integrity constraint failed (e.g., duplicate paths).")
        print("This usually means the database was not empty. Please delete the database file and try again.")
        session.close()
        return
    except Exception as e:
        session.rollback()
        print(f"An unexpected error occurred during folder indexing: {e}")
        session.close()
        return
    finally:
        session.close()

    # Phase 3: Find all Files (Parallel Scan)
    print(f"\nPhase 3: Starting parallel file scanning using {cpu_count()} cores...")

    # Create a list of (path, folder_id) tuples to send to the workers
    # This avoids the worker needing to do any DB lookups
    tasks = [(path, folder_id) for path, folder_id in path_to_id.items()]

    all_files_to_insert = []
    start_time_phase3 = time.time()

    try:
        # Use multiprocessing Pool to execute the file scanning in parallel
        # No initializer is needed as workers are stateless
        with Pool(processes=cpu_count()) as pool:

            # Use tqdm to wrap the pool.imap for real-time progress bar
            # pool.imap is memory-efficient for large numbers of tasks
            for file_list_chunk in tqdm(
                    pool.imap(process_directory_files, tasks),
                    total=len(tasks),
                    desc="Scanning Files (Parallel)"
            ):
                # file_list_chunk is the list of dicts returned by the worker
                all_files_to_insert.extend(file_list_chunk)

    except Exception as e:
        print(f"\nAn error occurred during parallel processing: {e}")

    end_time_phase3 = time.time()
    total_files_found = len(all_files_to_insert)
    print(f"Found {total_files_found} files in {end_time_phase3 - start_time_phase3:.2f}s.")

    # Phase 4: Insert all Files (Single-threaded Bulk Insert)
    print(f"\nPhase 4: Inserting {total_files_found} files into the database...")
    start_time_phase4 = time.time()
    session = Session()
    try:
        # Use bulk_insert_mappings for the list of dictionaries
        # This is the fastest way to insert many rows
        session.bulk_insert_mappings(File, all_files_to_insert)
        session.commit()
    except Exception as e:
        print(f"Error during bulk file insert: {e}")
        session.rollback()
    finally:
        session.close()

    end_time_phase4 = time.time()
    print(f"Database insertion complete in {end_time_phase4 - start_time_phase4:.2f}s.")

    # --- Summary ---
    total_time = (end_time_phase2 - start_time_phase2) + (end_time_phase3 - start_time_phase3) + (
                end_time_phase4 - start_time_phase4)
    print("\n--- Indexing Complete ---")
    print(f"Total time taken: {total_time:.2f} seconds")
    print(f"Total folders indexed: {len(path_to_id)}")
    print(f"Total files indexed: {total_files_found}")
    print(f"Database file: {DATABASE_NAME}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parallel File System Indexer using SQLAlchemy.")
    parser.add_argument('drive', type=str, help='The drive letter to scan (e.g., C, D, E).')

    args = parser.parse_args()

    # Check if the required libraries are installed
    try:
        from sqlalchemy import create_engine
        from tqdm import tqdm
    except ImportError:
        print("Error: Required libraries (SQLAlchemy, tqdm) are not installed.")
        print("Please install them using: pip install sqlalchemy tqdm")
        sys.exit(1)

    # Ensure the drive letter is a single character
    if len(args.drive) != 1 or not args.drive.isalpha():
        print("Error: Please provide a single drive letter (e.g., C).")
        sys.exit(1)

    main(args.drive.upper())