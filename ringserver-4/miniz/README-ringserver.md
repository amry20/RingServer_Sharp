# Miniz in ringserver

The [miniz](https://github.com/richgel999/miniz/) library is used in
ringserver for HTTP gzip encoding support.

## Updating the miniz version

The miniz release versions are shipped as a single source and header file.

Steps to update Mini-XML to a newer release:

1) Download a release bundle from: https://github.com/richgel999/miniz/releases
   Get the bundled .zip file, NOT the generated source archives
2) Extract the contents in a temporary directory
3) Copy the following files and directories to this directory:
   - `LICENSE`
   - `readme.md`
   - `miniz.c`
   - `miniz.h`
