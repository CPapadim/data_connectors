import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd

def redshift_ssh_query(query, 
                       db_name, db_user, db_pwd, db_host, db_port,
                       ssh_host=None, ssh_port=None,
                       ssh_username=None, ssh_private_key_file=None, ssh_private_key_password=None, ssh_password=None,
                       remote_bind_host='localhost', remote_bind_port=5432,
                       local_bind_host='localhost', local_bind_port=5432, ssh_query = True):
    
    """
    Runs the specified query on a database through an SSH tunnel
    
    Inputs:
        db_* - database credentials, host, and port
        Note that because we're tunneling through ssh, host should be localhost
        rather than the actual db host
        
        ssh_* - ssh server credentials, host, and port
        
        remote_bind_* - The remote host and port
        In this use-case the actual database host and port
        
        ssh_query - Whether or not to tunnel through SSH
    """
    
    conn_params = {
        'database': db_name,
        'user': db_user,
        'password': db_pwd,
        'host': db_host,
        'port': db_port
     }
    
    if ssh_query: 
      ssh_tunnel_params = {
          'ssh_username': ssh_username,
          'remote_bind_address': (remote_bind_host, remote_bind_port),
          'local_bind_address': (local_bind_host, local_bind_port)        
      }

      if ssh_private_key_file is not None:
          ssh_tunnel_params['ssh_pkey'] = ssh_private_key_file

      if ssh_private_key_password is not None:
          ssh_tunnel_params['ssh_private_key_password'] = ssh_private_key_password   

      if ssh_password is not None:
          ssh_tunnel_params['ssh_password'] = ssh_password

      with SSHTunnelForwarder((ssh_host, ssh_port), **ssh_tunnel_params) as server:

          server.start()
          print("Connected to SSH Server")

          conn = psycopg2.connect(**conn_params)
          print("Connected to Database")

          print("Executing Query...")
          df = pd.read_sql(query, conn)
          print("Query Completed")

          conn.close()
          print("Closed Database Connection")

          server.stop()
          print("Closed SSH Server Connection")
          
    else:
      
      conn = psycopg2.connect(**conn_params)
      print("Connected to Database")

      print("Executing Query...")
      df = pd.read_sql(query, conn)
      print("Query Completed")

      conn.close()
      print("Closed Database Connection")
      
return df 
