import sys

__all__ = [
    "path",
]

def path() -> str:
  bucket = sys.argv[1]
  eml = sys.argv[2]
  
  return "gs://" + bucket + "/" + eml + "/"

def bucket() -> str:
  return sys.argv[1]

def _databaseName() -> str:
  return sys.argv[3]

def _databaseUsername() -> str:
  return sys.argv[4]

def _databasePassword() -> str:
  return sys.argv[5]