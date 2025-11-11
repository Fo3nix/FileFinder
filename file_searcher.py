import sys
import argparse
# Third-party dependencies
# pip install sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

# --- Configuration (MUST match file_indexer.py) ---
DATABASE_NAME = 'filesystem_index.db'
Base = declarative_base()


# --- SQLAlchemy Models (MUST match file_indexer.py) ---

class Folder(Base):
    __tablename__ = 'folders'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('folders.id'), nullable=True)
    name = Column(String, nullable=False)
    path = Column(String, unique=True, nullable=False)
    parent = relationship("Folder", remote_side=[id])
    files = relationship("File", back_populates="folder")
    __table_args__ = (Index('idx_folder_name', 'name'),)


class File(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    folder_id = Column(Integer, ForeignKey('folders.id'), nullable=True)
    name = Column(String, nullable=False)
    folder = relationship("Folder", back_populates="files")
    __table_args__ = (Index('idx_file_name', 'name'),)


# --- Database and Setup Functions ---

def setup_db(database_url):
    """Initializes the database connection and returns the Session."""
    engine = create_engine(database_url)
    # Note: We don't need to call Base.metadata.create_all() as the indexer did that.
    Session = sessionmaker(bind=engine)
    return Session


def search_files(session, search_term):
    """
    Performs a case-insensitive LIKE search on the file names.
    The index on File.name ensures this query is fast.
    """
    # Use '%' as a wildcard in SQL LIKE queries
    like_query = f"%{search_term.lower()}%"

    # Perform a JOIN to get the full path from the Folder table
    # We use lower() for a case-insensitive search
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
                        help='The file name or pattern to search for (e.g., wallet, id_rsa, *secret*).')

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

    print(f"Searching for files matching: '{search_term}'...")

    start_time = time.time()
    results = search_files(session, search_term)
    end_time = time.time()

    print("\n--- Search Results ---")
    print(f"Query time: {end_time - start_time:.4f} seconds")
    print(f"Found {len(results)} results (limited to 1000).")

    if results:
        for file_obj, folder_obj in results:
            full_path = str(Path(folder_obj.path) / file_obj.name)
            print(f"-> {full_path}")
    else:
        print("No files matched your search query.")

    session.close()


if __name__ == '__main__':
    # We use a time import here so it doesn't need to be imported at the top
    import time

    main()