import os
import sys
import time
import argparse
from multiprocessing import Pool, cpu_count
from pathlib import Path

# Third-party dependencies
# pip install sqlalchemy tqdm
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

# --- Configuration ---
DATABASE_NAME = 'filesystem_index.db'
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

    # Index on the name for search speed (as requested)
    __table_args__ = (
        Index('idx_folder_name', 'name'),
    )

    def __repr__(self):
        return f"<Folder(name='{self.name}', path='{self.path}')>"


class File(Base):
    """Represents a file in the filesystem."""
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    folder_id = Column(Integer, ForeignKey('folders.id'), nullable=True)
    name = Column(String, nullable=False)

    # Relationships
    folder = relationship("Folder", back_populates="files")

    # Index on the name for search speed (as requested)
    __table_args__ = (
        Index('idx_file_name', 'name'),
    )

    def __repr__(self):
        return f"<File(name='{self.name}', folder_id='{self.folder_id}')>"


# --- Database and Setup Functions ---

def setup_db(database_url):
    """Initializes the database and returns the Session and Engine."""
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

    for root, dirs, files in os.walk(drive_path, onerror=lambda e: None):
        # Add the current root directory
        folder_paths.append(Path(root))
        # No need to iterate over dirs or files here, just collect roots

    print(f"Found {len(folder_paths)} directories.")
    return folder_paths


# --- Parallel Worker Function ---

def process_directory_files(directory_path_str):
    """
    Worker function run in parallel: finds files in a single directory
    and saves them to the database.
    """
    # Create a new session for this thread/process
    Session = process_directory_files.Session
    session = Session()

    try:
        directory_path = Path(directory_path_str)

        # 1. Look up the folder_id using the unique path field
        folder = session.query(Folder).filter_by(path=directory_path_str).one_or_none()

        if not folder:
            # Should not happen if Phase 2 runs correctly, but good for safety
            return 0

        folder_id = folder.id

        # 2. Find all files in the directory (non-recursive)
        files_to_add = []

        # os.scandir is generally faster than Path.iterdir() for raw speed
        # Filter for files only, skipping directories
        for entry in os.scandir(directory_path):
            if entry.is_file():
                files_to_add.append(File(folder_id=folder_id, name=entry.name))

        # 3. Bulk insert the files
        if files_to_add:
            session.bulk_save_objects(files_to_add)
            session.commit()
            return len(files_to_add)

        return 0

    except Exception as e:
        session.rollback()
        # Optionally log the error, but suppress file system access errors (which are common)
        # print(f"Error processing {directory_path_str}: {e}", file=sys.stderr)
        return 0

    finally:
        session.close()


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

    path_to_id = {}  # Dictionary to map full path to primary key ID

    # Sort paths to help determine parent/child relationships (A:\ vs A:\Foo)
    all_dir_paths.sort(key=lambda p: len(p.parts))

    try:
        for path in tqdm(all_dir_paths, desc="Indexing Folders"):
            path_str = str(path)
            # Determine parent_id
            parent_id = None
            if path.parent and str(path.parent) in path_to_id:
                parent_id = path_to_id[str(path.parent)]

            folder = Folder(
                name=path.name or str(path),  # Use path if name is empty (like C:\)
                path=path_str,
                parent_id=parent_id
            )
            session.add(folder)
            session.flush()  # Forces the ID generation before commit

            # Store the generated ID
            path_to_id[path_str] = folder.id

        session.commit()
        print(f"Successfully indexed {len(path_to_id)} folders.")

    except IntegrityError:
        session.rollback()
        print("Error: Integrity constraint failed (e.g., duplicate paths).")
        print("This usually means the database was not empty. Please delete the database file or use a clean one.")
        session.close()
        return
    except Exception as e:
        session.rollback()
        print(f"An unexpected error occurred during folder indexing: {e}")
        session.close()
        return
    finally:
        session.close()

    # Phase 3: Find and insert all Files (Parallel)
    print(f"\nPhase 3: Starting parallel file indexing using {cpu_count()} cores...")

    # Pass the Session factory object to the worker initialization
    def init_worker(Session):
        process_directory_files.Session = Session

    # Convert paths to strings for clean passing to the worker pool
    directory_path_strings = [str(p) for p in all_dir_paths]

    total_files_indexed = 0
    start_time = time.time()

    try:
        # Use multiprocessing Pool to execute the file scanning in parallel
        with Pool(processes=cpu_count(), initializer=init_worker, initargs=(Session,)) as pool:

            # Use tqdm to wrap the pool.imap for real-time progress bar
            for files_count in tqdm(
                    pool.imap(process_directory_files, directory_path_strings),
                    total=len(directory_path_strings),
                    desc="Indexing Files (Parallel)"
            ):
                total_files_indexed += files_count

    except Exception as e:
        print(f"\nAn error occurred during parallel processing: {e}")

    end_time = time.time()

    print("\n--- Indexing Complete ---")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")
    print(f"Total folders indexed: {len(path_to_id)}")
    print(f"Total files indexed: {total_files_indexed}")
    print(f"Database file: {DATABASE_NAME}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parallel File System Indexer using SQLAlchemy.")
    parser.add_argument('drive', type=str, help='The drive letter to scan (e.g., C, D, E).')

    args = parser.parse_args()

    # Check if the required library is installed
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print("Error: SQLAlchemy is not installed.")
        print("Please install it using: pip install sqlalchemy tqdm")
        sys.exit(1)

    # Ensure the drive letter is a single character
    if len(args.drive) != 1 or not args.drive.isalpha():
        print("Error: Please provide a single drive letter (e.g., C).")
        sys.exit(1)

    main(args.drive.upper())