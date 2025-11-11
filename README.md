# FileFinder

FileFinder is a high-performance, parallel filesystem indexer and search tool. It is designed to rapidly scan an entire drive, store the file and folder structure in a local SQLite database, and provide a near-instantaneous search script to query that index.

This project is built using `sqlalchemy` for database management and `uv` for package and environment management.

## 🚀 Core Features

  * **Parallel Indexing:** The `file_indexer.py` script uses `multiprocessing` to scan directory contents in parallel, dramatically speeding up the initial scan of a large drive.
  * [cite\_start]**Efficient Database:** Uses SQLite via `sqlalchemy`, creating a single-file database (`filesystem_index.db`) [cite: 2] that is easy to manage and back up.
  * **Intelligent Search:** The `file_searcher.py` script features two search modes:
      * **Fast (Indexed) Search:** Default mode for queries *without* wildcards (e.g., `report.docx`). This uses the database index for a "starts-with" search, which is extremely fast.
      * **Slow (Wildcard) Search:** Supports `*` and `?` for full wildcard searches (e.g., `*report*` or `data_??.log`). This is slower as it must scan the table, but allows for more flexible queries.
  * **Robust:** Gracefully handles filesystem permission errors during scanning and sanitizes filenames with encoding issues.
  * [cite\_start]**Modern Tooling:** Uses Python 3.12+ [cite: 1, 3] and is managed entirely with `uv`.

-----

## 🔧 Installation

This project is managed with `uv`.

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/Fo3nix/FileFinder.git
    cd filefinder
    ```

2.  **Ensure `uv` is installed:**
    If you don't have it, you can install it via pip:

    ```bash
    pip install uv
    ```

3.  **Create the virtual environment and sync dependencies:**
    [cite\_start]This command creates a `.venv` folder and installs all dependencies from `uv.lock`[cite: 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14].

    ```bash
    uv sync
    ```

4.  **Activate the virtual environment:**

      * **Windows:**
        ```powershell
        .venv\Scripts\activate
        ```
      * **macOS/Linux:**
        ```bash
        source .venv/bin/activate
        ```

-----

## ⚙️ Usage

Using FileFinder is a two-step process: you must **index** the drive first, and then you can **search** it.

### Step 1: Indexing a Drive

Run the `file_indexer.py` script, providing the drive letter you wish to scan.

> **Note:** The indexer script is designed for Windows drives (e.g., `C`, `D`).

```bash
# Index the entire 'C' drive
python file_indexer.py C
```

This process may take several minutes depending on the size of your drive and the number of files. It will show a progress bar via `tqdm`. When finished, a `filesystem_index.db` file will be created in the project root.

#### Re-indexing Files

If you want to update the file index (Phase 3 & 4) without re-scanning the entire folder tree (Phase 1 & 2), you can use the `--skip-folders` flag. This is much faster for frequent updates.

```bash
# Re-scan for files only, using the existing folder structure in the DB
python file_indexer.py C --skip-folders
```

### Step 2: Searching the Index

Once the database is created, use the `file_searcher.py` script to find files.

#### ⚡ Fast Search (Starts-With)

This is the default and recommended mode. It's extremely fast because it uses the database index.

```bash
# Finds "report.docx", "report-final.pdf", etc.
python file_searcher.py "report"
```

```
Searching for files matching query: 'report'...
(Using fast, indexed search: 'name LIKE "report%"')
...
-> C:\Users\You\Documents\report.docx
-> C:\Users\You\Downloads\report-final.pdf
```

#### 🐌 Slow Search (Wildcard)

Use `*` (matches any characters) or `?` (matches a single character) to perform a full wildcard search. The script will warn you that this is a slower, non-indexed query.

```bash
# Finds any file containing "secret"
python file_searcher.py "*secret*"
```

```
Searching for files matching query: '*secret*'...
!! (Using slow, non-indexed search: 'name LIKE "%secret%"')
!! (This query may be slow as it cannot use the index effectively)
...
-> C:\Projects\old\my_secret_notes.txt
-> D:\Backup\top_secret_plans.zip
```

-----

## 🛠️ How the Indexer Works

The `file_indexer.py` script is optimized by splitting its work into four phases:

1.  **Phase 1: Collect Directories (Single-threaded)**
    Performs a fast, single-threaded `os.walk` to collect a complete list of all accessible directory paths on the drive.

2.  **Phase 2: Index Folders (Single-threaded)**
    Inserts all directories into the `folders` table. This is done single-threaded to correctly resolve `parent_id` relationships, ensuring the tree structure is maintained.

3.  **Phase 3: Parallel File Scan (Multi-threaded)**
    This is the core optimization. A `multiprocessing.Pool` is created with all available CPU cores. The list of directories from Phase 2 is divided among the worker processes, which scan for files in parallel.

4.  **Phase 4: Bulk Insert Files (Single-threaded)**
    The lists of files from all parallel workers are collected and inserted into the `files` table in a single, highly-efficient `bulk_insert_mappings` operation.

-----

## 📦 Project Dependencies

  * **`pyproject.toml`**: Defines the project and its dependencies:
      * `sqlalchemy`: For all database operations (ORM, schema, connections).
      * `tqdm`: For clean, visual progress bars during indexing.
  * [cite\_start]**`uv.lock`**: The lockfile generated by `uv`, ensuring reproducible builds by locking all direct and transitive dependencies (like `greenlet`, `colorama`, etc.)[cite: 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14].


DISCLAMER: this project was generated using LLMs, including this readme

