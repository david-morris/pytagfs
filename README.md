# PyTagFS
PyTagFS is a tag-based filesystem written in Python.

PyTagFS is intended as a proof-of-concept for a tag-based filesystem. In this stage of development, expect data loss.

## Wish List
- [ ] Basic functionality
  - [x] CRUD operations
    - [x] Create
      - [x] In the root folder
      - [x] In tags
    - [x] Read
    - [x] Update
      - [x] Overwrite
      - [x] Move
        - [x] Out of the root directory
        - [x] Out of a subdirectory
    - [x] Delete
  - [ ] Must work with file managers and over SMB
    - [x] Create
    - [x] Read
    - [x] Update
    - [ ] Delete
- [ ] Odds and Ends
  - [ ] getxattr
  - [ ] symlinks
    - [ ] absolute path
    - [ ] magic files to handle being read from different paths
  - [ ] clean up spurious dialogs over SMB
  - [ ] hide empty 'subdirectories'
- [ ] Better backend
  - [ ] Switch to SQLite without a wrapper
  - [ ] Write ACID consistency guarantees
  - [ ] Make operations atomic
  - [ ] Make commits tuneable
  - [ ] Turn files into sqlite blobs
- [ ] Possible reimplementation
  - [ ] Rust seems like a good target
