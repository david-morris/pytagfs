# PyTagFS
PyTagFS is a tag-based filesystem written in Python. Instead of directories being 'containers,' they represent attributes, so they can be accessed in any order. It's intended for organizing diverse media collections.  You'll never forget whether you put that picture of a sunset with a whale breaching in `~/Pictures/landscapes/sunsets` or `~/Pictures/animals/wild` again!

This is early beta software. EXPECT IT TO LOSE YOUR DATA. I only put symlinks in, so my actual files stay safe.

This is an unusual file system, which might cause file managers to make more low-level filesystem calls than usual, which may increase disk wear.
## Usage

Start pytagfs with:
```$ pytagfs -m <mountpoint> -d <datastore folder> [-o <comma-separated filesystem args>] [<flag>...]```
Make sure that `mountpoint` and `datastore folder` refer to folders which exist, that the datastore folder is not inside the mountpoint, and is empty the first time you run the command.

For most usage, you shouldn't need any flags other than maybe `-o allow_other` if you are sharing the filesystem over SMB. Please collect logs with the `-v -s` and then `-vv` options and raise an issue if you notice something that doesn't work the way it should.

### Basic Usage

In pytagfs, files are files, and folders are tags. That means I can put a file in `mountpoint/peru2018/pictures/landscapes/` and it will be in `mountpoint/landscapes` (but as a hidden file). Also, when I put a pdf of my ticket receipt in `mountpoint/peru2018/paperwork/`, `mountpoint/paperwork/peru2018/` now is non-hidden and contains that pdf.

Those 'folders,' which I will refer to as tags, must be created as you would normally create folders.

### Gotchas

You may not have multiple files anywhere in one tag filesystem with the same name. 

File and tag names may not start or end with a `.`. There is no way to choose whether a file or tag is displayed hidden or not.

Stopping or restarting a FUSE mountpoint locks up any shells currently working inside it.

### Hidden Files

A file is hidden when your path does not include all their tags, but all tags in your path do apply to the file. When your path contains exactly the tags of a file, it is shown non-hidden. This is because most file managers let you quickly toggle whether hidden files are shown, so you can use that property to quickly see if you have all the tags you want on a specific file or files.

A tag is hidden when adding it to your path would mean you have no more matching files. However, tags are never hidden directly inside the mountpoint.

Files and tags are hidden by adding a `.` to the beginning of the name. The names then update as circumstances change, for example, drag `mountpoint/peru2018/streetcaricature.jpg` into `mountpoint/peru2018/.portraits/` and that directory `mountpoint/peru2018/` now contains `.streetcaricature.jpg` (now hidden) and `portraits/` (no longer hidden).

### Moving

Moving a hidden file only adds tags. Moving a non-hidden file changes the tags to exactly the destination path tags. You can rename a file.  You can rename and move a file at the same time.

You can rename a tag. Moving a tag inside another tag has no meaning.

### Symlinks

Pytagfs supports symlinks. Make sure to make your relative symlinks in the mountpoint directory. You can read them from any path, the read link path will be adjusted based on the number of tags.

### Deleting

Deleting files directly inside the mountpoint deletes them from the filesystem. Deleting them inside tags removes the last tag in your path from the file.

Empty tags can be deleted with `rmdir`. If your file manager refuses to try to delete a directory with items inside of it, or tries to delete recursively, you can also delete an empty tag by renaming it in the mountpoint to `..deleteme`. This is a useful workaround if you are using a file manager or SMB.

## Installation
Requirements: FUSE. That means pytagfs only supports Unix-like systems.

Pip requirements: fusepy

Coming Soon: Installation via pip.


## Wish List
- [x] Basic functionality
  - [x] CRUD operations
  - [x] Must work with file managers and over SMB
- [ ] Odds and Ends
  - [ ] Add options to limit the number of hidden files you list (for large media collections).
  - [ ] Consider giving tags their own inodes or otherwise managing permissions, attrs, xattrs
  - [ ] Consider turning files into sqlite blobs
  - [ ] If you have experience with SQL, consider checking over my queries.
- [ ] Possible reimplementation
  - [ ] Rust seems like a good target
