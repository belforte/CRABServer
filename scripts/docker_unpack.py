#!/usr/bin/env python
from __future__ import print_function
from json import loads
from sys import exit, version_info
import stat
from os.path import exists, join
from os import walk, lstat, chmod
if version_info[0] == 2:
  from commands import getstatusoutput as run_cmd
else:
  from subprocess import getstatusoutput as run_cmd

def cleanup_exit(msg, tmpdirs=None, image_hash="", exit_code=1):
  if not tmpdirs:
    tmpdirs = []
  if msg: print(msg)
  for tdir in tmpdirs: run_cmd("rm -rf %s" % tdir)
  if image_hash: run_cmd("docker rm -f %s" % image_hash)
  exit(exit_code)

def fix_mode(full_fname, mode_num, st=None):
  if not st: st = lstat(full_fname)
  n_mode = mode_num*100
  #mode = mode_num + mode_num*10 + n_mode
  old_mode = stat.S_IMODE(st.st_mode)
  if (old_mode & n_mode) == 0:
    new_mode = old_mode | n_mode
    print("Fixing mode of: %s: %s -> %s" % (full_fname, oct(old_mode), oct(new_mode)))
    chmod(full_fname, new_mode)

def fix_modes(img_dir):
  # Walk the path, fixing file permissions
  for (dirpath, dirnames, filenames) in walk(img_dir):
    for fname in filenames:
      fix_mode (join(dirpath, fname), 4)
    for dname in dirnames:
      full_dname = join(dirpath, dname)
      st = lstat(full_dname)
      if not stat.S_ISDIR(st.st_mode): continue
      old_mode = stat.S_IMODE(st.st_mode)
      if old_mode & 0o111 == 0:
        fix_mode (full_dname, 1, st)
      if old_mode & 0o222 == 0:
        fix_mode (full_dname, 2, st)
  return

def process(image, outdir):  
  container = image.split(":",1)[0]
  tag = image.split(":",1)[-1]
  if container==tag: tag="latest"

  e, image_hash = run_cmd("docker pull %s 2>&1 >/dev/null; docker images %s | grep '^%s \|/%s ' | grep ' %s ' | tail -1 | sed 's|  *|:|g' | cut -d: -f3" % (image, container, container, container, tag))
  print ("Image hash: %s" % image_hash)
  if e:
    print(image_hash)
    exit(1)

  img_sdir = join(".images", image_hash[0:2], image_hash)
  img_dir = join(outdir, img_sdir)
  if exists(img_dir): return

  print ("Starting Container %s with %s hash" % (image, image_hash))
  tmpdir = join(outdir, ".images", "tmp")
  e, o = run_cmd('docker run -u $(id -u):$(id -g) --name %s %s echo OK' % (image_hash,image))
  if e: cleanup_exit(o, [tmpdir], image_hash)

  print("Getting Container Id")
  e, o = run_cmd('docker ps -aq --filter name=%s' % image_hash)
  if e: cleanup_exit(o, [tmpdir], image_hash)
  container_id = o

  print("Exporting Container ",container_id)
  e, o = run_cmd('rm -rf %s; mkdir -p %s; cd %s; docker export -o %s.tar %s' % (tmpdir, tmpdir, tmpdir,image_hash, container_id))
  if e: cleanup_exit(o, [tmpdir], image_hash)

  print("Cleaning up container ",image_hash)
  run_cmd('docker rm -f %s' % image_hash)

  print("Unpacking exported container ....")
  e, o = run_cmd('mkdir -p %s; cd %s; tar -xf %s/%s.tar' % (img_dir, img_dir, tmpdir, image_hash))
  if e: cleanup_exit(o, [tmpdir, img_dir])
  run_cmd('rm -rf %s' % tmpdir)

  for xdir in [ "srv", "cvmfs", "dev", "proc", "sys", "build", "data", "pool" ]:
    sdir = join(img_dir, xdir)
    if not exists(sdir): run_cmd('mkdir %s' % sdir)
  
  print("Fixing file modes ....")
  fix_modes (img_dir)

if __name__ == "__main__":
  from optparse import OptionParser
  parser = OptionParser(usage="%prog <pull-request-id>")
  parser.add_option("-c", "--container",  dest="container",  help="Docker container e.g. cmssw/cc7:latest", default=None)
  parser.add_option("-o", "--outdir",     dest="outdir",     help="Output directory where to unpack image", default=None)
  opts, args = parser.parse_args()

  if len(args) != 0: parser.error("Too many/few arguments")
  process(opts.container, opts.outdir)
  print("All OK")
  
