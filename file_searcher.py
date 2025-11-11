import sys
import argparse
import time
from pathlib import Path

# Third-party dependencies
# pip install sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

# --- Configuration (MUST match file_indexer.py) ---
DATABASE_NAME = 'filesystem_index.db'
# Use modern SQLAlchemy 2.0 syntax for Base
Base = declarative_base()


# --- SQLAlchemy Models (MUST match file_indexer.py) ---

class Folder(Base):
    """Represents a directory in the filesystem."""
    __tablename__ = 'folders'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('folders.id'), nullable=True)
    name = Column(String, nullable=False)
    path = Column(String, unique=True, nullable=False)

    # Relationships
    parent = relationship("Folder", remote_side=[id])
    files = relationship("File", back_populates="folder")

    __table_args__ = (Index('idx_folder_name', 'name'),)


class File(Base):
    """Represents a file in the filesystem."""
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    # A file must belong to a folder, so nullable=False
    # (FIXED: Was incorrectly set to nullable=True)
    folder_id = Column(Integer, ForeignKey('folders.id'), nullable=False)
    name = Column(String, nullable=False)

    # Relationships
    folder = relationship("Folder", back_populates="files")

    __table_args__ = (Index('idx_file_name', 'name'),)


# --- Database and Setup Functions ---

def setup_db(database_url):
    """Initializes the database connection and returns the Session."""
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    return Session


def search_files(session, search_term):
    """
    Performs a case-insensitive LIKE search on the file names.
    """

    # --- NEW SEARCH LOGIC ---
    # If the user added their own wildcards, respect them.
    # The '*' is a common user-friendly wildcard, replace with SQL's '%'
    if '*' in search_term or '?' in search_term:
        # Replace user wildcards with SQL wildcards
        like_query = search_term.replace('*', '%').replace('?', '_')
    else:
        # If no wildcards, assume a "starts with" search.
        # This is very fast and uses the index.
        like_query = f"{search_term}%"

    if not like_query.startswith('%') and not like_query.startswith('_'):
        print(f"(Using fast, indexed search: 'name LIKE \"{like_query}\"')")
    else:
        print(f"!! (Using slow, non-indexed search: 'name LIKE \"{like_query}\"')")
        print("!! (This query may be slow as it cannot use the index effectively)")
    # --- END NEW LOGIC ---

    # Perform a JOIN to get the full path from the Folder table
    # We use .ilike() for a case-insensitive search
    results = session.query(File, Folder) \
        .join(Folder, File.folder_id == Folder.id) \
        .filter(File.name.ilike(like_query)) \
        .limit(1000) \
        .all()

    return results


# --- Main Logic ---

def main():
    parser = argparse.ArgumentParser(description="File System Searcher for indexed SQLite DB.")
    parser.add_argument('query', type=str,
                        help="The file name to search for. "
                             "e.g., 'wallet' (fast search for 'wallet%'). "
                             "e.g., '*wallet.dat' or '*secret*' (slow search for '%wallet.dat' or '%secret%')")

    args = parser.parse_args()
    search_term = args.query

    # Check if the required library is installed
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print("Error: SQLAlchemy is not installed.")
        print("Please install it using: pip install sqlalchemy")
        sys.exit(1)

    database_url = f'sqlite:///{DATABASE_NAME}'

    try:
        Session = setup_db(database_url)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print(f"Please ensure '{DATABASE_NAME}' was created by running 'file_indexer.py' first.")
        sys.exit(1)

    session = Session()

    print(f"Searching for files matching query: '{search_term}'...")

    start_time = time.time()
    results = search_files(session, search_term)
    end_time = time.time()

    print("\n--- Search Results ---")
    print(f"Query time: {end_time - start_time:.4f} seconds")
    print(f"Found {len(results)} results (limited to 1000).")

    if results:
        for file_obj, folder_obj in results:
            # (FIXED: 'Path' is now imported)
            full_path = str(Path(folder_obj.path) / file_obj.name)
            print(f"-> {full_path}")
    else:
        print("No files matched your search query.")

    session.close()


if __name__ == '__main__':
    # (FIXED: 'time' import moved to top)
    main()