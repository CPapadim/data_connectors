import os, boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from re import sub
import tarfile
import io


def get_s3_client(bucket,
                  access = None,
                  secret = None,
                  use_creds = False):
    
    """
    
    Initialize s3 connection client
    
    """
    
    if use_creds:
        s3 = boto3.client(service_name = 's3', 
                          aws_access_key_id = access, 
                          aws_secret_access_key = secret)
    else:
        s3 = boto3.client(service_name = 's3', 
                          config=Config(signature_version=UNSIGNED))
    
    return s3, bucket
    
def s3_CSVtoDF(bucket, file_name, use_creds = False, access = None, secret = None, **kwargs):
    
    """
    
    Stream a data file from S3 into a dataframe
    
    All **kwargs are passed to pandas.read_csv() and must
    therefore be valid keyword arguments of that function
    
    """
    
    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    
    return pd.read_csv(io.BytesIO(obj.response['Body'].read()), **kwargs)

def s3_XLStoDF(bucket, file_name, use_creds = False, access = None, secret = None, **kwargs):
    
    """
    
    Stream a data file from S3 into a dataframe
    
    All **kwargs are passed to pandas.read_excel() and must
    therefore be valid keyword arguments of that function
    
    """
    
    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    
    return pd.read_excel(io.BytesIO(obj.response['Body'].read()), **kwargs)

  
def push_file_to_s3(bucket, path, key=None, use_creds = False, access = None, secret = None):
    
    """Take in a path and push to S3"""

    if key == None:
        print("Can't push to S3 without a key. Please specify a key.")
        return

    key = key.replace(' ','-')

    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    
    s3.upload_file(path, bucket, key)
    print("Sent file %s to S3 with key '%s'"%(path,key))
        
        
def pull_file_from_s3(bucket, key, path, use_creds = False, access = None, secret = None):
    
    local_dir = '/'.join(path.split('/')[:-1])
    if not os.path.isdir(local_dir) and local_dir != '':
        print("Local directory %s doesn't exist"%(local_dir))
        return
    
    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    s3.download_file(bucket, key, path)
    

    print("Grabbed %s from S3. Local file %s is now available."%(key,path))
            
def s3_fetch_module(bucket, key, file_name, use_creds = False, access = None, secret = None):
    
    """
    Fetch a module in a tar.gz file from s3
    
    """
    
    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    print('Fetching ' + key + file_name)
    s3.download_file(bucket, key + file_name, Filename=file_name)

                
    dir_name = sub('.tar.gz$', '', file_name)
    
    contains_hyphen = False
    if '-' in dir_name:
        contains_hyphen = True
        print("Module name contains invalid '-' hyphens.  Replacing with '_' underscores")
        dir_name = dir_name.replace('-','_')
    
    
    try:
        shutil.rmtree('./' + dir_name)
        print('Removing old module ' + dir_name)
    except:
        pass
    
    print('Extracting ' + file_name + ' into ' + dir_name)
    archive = tarfile.open(file_name, 'r:gz')
    archive.extractall('./')
    archive.close()
    
    if contains_hyphen:
        os.rename(dir_name.replace('_','-'), dir_name)
        
    try:
        os.remove(file_name)
        print('Removing ' + file_name)
    except:
        pass
    
    if ~os.path.exists(dir_name + '/__init__.py'):
        print('__init__.py not found.  Creating it in ' + dir_name)
        open(dir_name + '/__init__.py','w').close()
        
def s3_ls(bucket, path, use_creds = False, access = None, secret = None):
    
    """
    
    List contents of s3 directory specified in 'path'
    
    """
    
    s3, bucket = get_s3_client(bucket, access = access, secret = secret, use_creds = use_creds)
    
    if path != '' and path[-1] != '/':
        path += '/'
        
    files = []
    directories = []
    
    try:
        for fname in s3.list_objects(Bucket=bucket, Prefix = path)['Contents']:
            
            if '/' not in fname['Key'].replace(path,''):
                files.append(fname['Key'].replace(path,''))
            elif fname['Key'].replace(path,'').split('/')[0] + '/' not in directories:
                directories.append(fname['Key'].replace(path,'').split('/')[0] + '/')
                
    except KeyError:
        return 'Directory Not Found'

    return directories + files
