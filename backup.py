import argparse, sys, re, tempfile

from configparser import RawConfigParser
from pysftp import Connection

from subprocess import Popen
from os.path import basename, join
from datetime import datetime

class Config(RawConfigParser):

  def __init__(self, config_file):
    RawConfigParser.__init__(self)
    self.readfp(config_file)


class KEYS(object):
  UNAME = "USERNAME"
  PWD = "PASSWORD"
  HOST = "HOST"
  PORT = "PORT"

  DB_NAME = "DB_NAME"

  FOLDER = "FOLDER"

  FMT = "BACKUP_FILENAME_FORMAT"
  DATE_FMT = "BACKUP_DATE_FORMAT"

  DATE_REGEX = "BACKUP_DATE_REGEX"
  DBNAME_REGEX = "BACKUP_DBNAME_REGEX"

def group_ftp_content(content, config, n):

  fmt = config.get("General", KEYS.FMT)
  filter_regex = re.compile(fmt.format(
    dbname = config.get("General", KEYS.DBNAME_REGEX),
    date = config.get("General", KEYS.DATE_REGEX)))

  content = filter(filter_regex.match, content)

  sorting_regex = re.compile(fmt.replace("{date}", "({date})").format(
    dbname = config.get("General", KEYS.DBNAME_REGEX),
    date = config.get("General", KEYS.DATE_REGEX)))


  sorted_content = [(datetime.strptime(sorting_regex.match(fname).group(1), config.get("General", KEYS.DATE_FMT)), fname) for fname in content]
  sorted_content = [c[1] for c in sorted(sorted_content, key = lambda entry: entry[0], reverse = True)]

  return sorted_content[:n - 1], sorted_content[n - 1:]

def do_backup(sftp, config):
  backup = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
  command = "mysqldump --user={uname} --password={pwd} --host={host} --port={port} {dbname}"
  command = command.format(
    uname = config.get("MySQL", KEYS.UNAME),
    pwd = config.get("MySQL", KEYS.PWD),
    host = config.get("MySQL", KEYS.HOST),
    port = config.get("MySQL", KEYS.PORT),
    dbname = config.get("MySQL", KEYS.DB_NAME),
    )
  print("\tRunning: {}".format(command.replace(config.get("MySQL", KEYS.PWD), "******")))
  

  process = Popen(command.split(), stdout = backup)
  process.wait()

  fmt = config.get("General", KEYS.FMT)
  fname = fmt.format(
    dbname = config.get("MySQL", KEYS.DB_NAME),
    date = datetime.now().strftime(config.get("General", KEYS.DATE_FMT)),
    )
  backup_path = join(config.get("SFTP", KEYS.FOLDER), fname)

  backup.seek(0)
  print("Dumping to: {}".format(backup_path))
  sftp.putfo(backup, backup_path)

def delete_old_backups(sftp, backups):
  for backuppath in backups:
    sftp.remove(backuppath)




def get_args():
  parser = argparse.ArgumentParser(
    prog = basename(__file__),
    description = "simple script for MySQL-DB backup to a FTP server")

  parser.add_argument(
    "-c", "--config",
    help = "Config file with passwords and usernames",
    required = True,
    type=argparse.FileType('r'))

  parser.add_argument(
    "-n", "--number-of-backups",
    help = "Number of backups to leave on the server",
    required = True,
    default = 10,
    type = int)

  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  config = Config(args.config)


  sftp = Connection(
    host = config.get("SFTP", KEYS.HOST),
    username = config.get("SFTP", KEYS.UNAME),
    password = config.get("SFTP", KEYS.PWD),
  )

  ftp_folder = config.get("SFTP", KEYS.FOLDER)

  dir_content = sftp.listdir(ftp_folder)

  keep, delete = group_ftp_content(dir_content, config, args.number_of_backups)

  do_backup(sftp, config)
  delete_old_backups(sftp, map(lambda fname: join(ftp_folder, fname), delete))
