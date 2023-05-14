#!/usr/bin/env python
import sys
import logging
from fuse import FUSE
import stat

from optparse import OptionParser

from src.tagfs import Tagfs

def main(mountpoint, root, options, flat_delete, limit):
    logging.info("Mountpoint: "+ str(mountpoint)+ ", root: "+ str(root))
    FUSE(Tagfs(root, mountpoint, flat_delete, limit), mountpoint, nothreads=True, foreground=True, **options)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="count", dest="verbosity", default=0,
                      help="print information about interesting calls")
    parser.add_option("-s", "--show_fusepy_errors", action="store_false", dest="silent", default=True,
                      help="print normal fusepy errors without high verbosity")
    parser.add_option("-m", "--mountpoint", dest="mountpoint",
                      help="mountpoint of the tag filesystem")
    parser.add_option("-d", "--datastore", dest="datastore",
                      help="Data store directory for the tag filesystem")
    parser.add_option("-o", "--options", dest="fuse_options",
                      help="FUSE filesystem options")
    parser.add_option("-a", "--anywhere-delete", dest="flat_delete", action="store_false", default=True,
                      help="allow deletion anywhere, instead of just in the root of the fileystem")
    parser.add_option("-l", "--limit", dest="limit", type="int", default=-1,
                      help="set a limit to the number of hidden files to list in the root of the mount")
    options, args = parser.parse_args()
    if options.verbosity > 0:
        logging.root.setLevel(logging.INFO)
        if options.verbosity > 1:
            logging.root.setLevel(logging.DEBUG)
        logging.info("Verbosity: "+ str(options.verbosity))
    if options.silent:
        class DevNull:
            def write(self, msg):
                pass
        sys.stderr = DevNull()
        sys.tracebacklimit = 0

    if options.fuse_options is not None:
        kwargs = {opt: True for opt in options.fuse_options.split(",")}
        logging.info("FS options: " + str(kwargs))
    else:
        kwargs = {}
    main(options.mountpoint, options.datastore, kwargs, options.flat_delete, options.limit)
