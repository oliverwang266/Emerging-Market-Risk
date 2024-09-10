'''
Purpose: 
This script is used to interact with the HBS Grid. 
It could help automize the process of running codes and 
syncing files on the HBS Grid.

In the main function, it will upload the code to the server,
and then run the layout analysis code on the server.

Details:
1. The Grid class is the main class to interact with the HBS Grid.
    - We utilize the paramiko library to establish the SSH connection.
    - It could run commands on the server, including bjobs, bsub, bkill.
    - With the sftp connection, it could download/upload files and folders.
2. Something to note:
    - The Grid class is basically a wrapper of the paramiko library.
    It runs commands on the server directly, so the property of commands is the same as the server.
        - For interactive jobs, the job will terminate once the connection is closed.
        So, we need to keep the connection open if you want to keep the job running.
        - For batch jobs, the job will keep running even if the connection is closed.
    - *The bjobs command may not return output every time. If it has no output,
    it will raise an exception. In this case, we need to wait for a while and try again.
    - Only batch jobs are well tested. Interactive jobs may have some issues about dynamic output.
    So, if you want to run an interactive job, please do not run it for important tasks without testing.
    - We may split the class into multile classes in the future to make it more modular.
    And use composition design pattern to combine them. It will not affect the current usage.
    - Some dangerout functions will be hide only for internal usage.
    - Bugs report and suggestions are welcome!

Author: Xiaoyu Chen
Date: August 16, 2024

'''


import paramiko
import time
import pandas as pd
import os
import stat
import re

class Grid:
    '''
    
    '''
    def __init__(self, user_name, password, host_name='hbsgrid.hbs.edu', quiet='True'):
        '''
        Initialize the Grid object.

        Args:
            user_name (str): The user name to login the server.
            password (str): The password to login the server.
            host_name (str): The host name of the server. Default is 'hbsgrid.hbs.edu'.
            quiet (bool): Whether to print the executing commands and outputs.
        '''
        # Initialize the connection
        self.host_name = host_name
        self.user_name = user_name
        self.password = password
        # Initialize the ssh and sftp object
        self.ssh = None # For command execution
        self.sftp = None # For file operation
        # We may use logging module in the future,
        # But for simplicity, we use a quiet flag to control the output.
        self.quiet = quiet

    def connect(self):
        # Create a ssh object
        self.ssh = paramiko.SSHClient()
        # Add key
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Connect to the server
        self.ssh.connect(hostname=self.host_name, port=22, username=self.user_name, password=self.password)

    def open_sftp(self):
        if self.ssh is None:
            raise Exception("SSH connection not established. Call connect() first.")
        self.sftp = self.ssh.open_sftp()

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    # Context Manager
    # You can use the with statement to ensure the connection is closed properly.
    # Usage:
    # with Grid(user_name, password) as rcs:
    #     rcs.exec_command('bjobs')

    def __enter__(self):
        self.connect()
        self.open_sftp()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_job_id(self, text):
        match = re.search(r'Job <(\d+)> is submitted', text)
        if match:
            job_number = match.group(1)
            return int(job_number)
        else:
            return None

    def exec_command(self, command, pseudo_terminal=False):
        '''
        Execute a command on the server.

        Args:
            command (str): The command to execute on the server.
            pseudo_terminal (bool): Whether to request a pseudo-terminal to read output in real-time.

        Return:
            str: The output of the command.
        '''

        if self.ssh is None:
            raise Exception("SSH connection not established. Call connect() first.")
        if not self.quiet:
            print(f'Executing: {command}')
        output_str = ""
        error_str = ""

        transport = self.ssh.get_transport()
        channel = transport.open_session() # Open a session to run the command

        if pseudo_terminal:
            channel.get_pty()  # Request a pseudo-terminal to read output in real-time

        channel.exec_command(command) # Execute the command

        while True:
            if channel.recv_ready(): # Check if there is output
                time.sleep(0.5) # Sleep for a while to wait for more output
                temp_output = channel.recv(8192).decode('utf-8') # Read the output
                output_str += temp_output # Append the output
                if not self.quiet: # Print the output
                    print(temp_output, end='')

            # Same for the error output
            if channel.recv_stderr_ready():
                time.sleep(0.5)
                temp_error = channel.recv_stderr(8192).decode('utf-8')
                error_str += temp_error
                if not self.quiet:
                    print(temp_error, end='')

            # Break the loop if the command is finished
            if channel.exit_status_ready():
                time.sleep(0.5)
                break

        # If there is an error, print the error
        # I choose not to raise and exception,
        # because I do not want the program to stop.
        if error_str:
            output_str += "\n" + error_str

        return self.get_job_id(output_str)
    
    def exec_commands(self, commands):
        command = ' && '.join(commands)
        return self.get_job_id(self.exec_command(command))
    
    def exec_with_conda(self, conda_env, script):
        '''
        Execute a command with the specified conda environment.

        Args:
            conda_env (str): The conda environment to activate.
            command (str): The command to run.

        Return:
            str: The output of the command.
        '''

        return self.exec_commands([f'conda activate {conda_env}', script])
    
    def get_bjobs(self):
        '''
        Get the running jobs on the server.
        Return a pandas DataFrame with the job information.

        Return:
        pd.DataFrame: The running jobs on the server.
        '''
        time.sleep(5) # Wait for a while to get the output
        output = self.exec_command('bjobs')
        if not output:
            raise Exception('No output from bjobs, wait for a while and try again.')
        
        if 'No unfinished job found' in output:
            return pd.DataFrame()

        # Parse the output
        lines = output.split('\n')
        columns_str = lines[0]
        columns = lines[0].split()

        rows = lines[1:-1]

        res = []
        # Find the start index of each column names in the output string
        start_idx = []
        for char_idx, char in enumerate(columns_str):
            if char_idx == 0:
                if char != " ":
                    start_idx.append(char_idx)
            else:
                if char != " " and columns_str[char_idx-1] == " ":
                    start_idx.append(char_idx)

        # Use the index to split the output string
        for line in rows:
            values = []
            for i, idx in enumerate(start_idx):
                if i == len(start_idx) - 1:
                    temp = line[idx:]
                else:
                    temp = line[idx:start_idx[i+1]]
                values.append(temp.strip())
            res.append(dict(zip(columns, values)))

        return pd.DataFrame(res)
    
    def get_bsub_command(self, queue, mem, cpu_num, script):
        '''
        Assemble the bsub command with the given parameters.

        Args:
        queue (str): The queue to submit the job.
        mem (int): The memory in GB.
        cpu_num (int): The number of CPUs.
        script (str): The script to run.

        Return:
        str: The bsub command.
        '''
        cmds = ['bsub']

        queue_cmd = ['-q', queue]

        interactive = [
            'long_int',
            'short_int',
            'sas_int',
            'gpu_int'
        ]

        batch = [
            'long',
            'short',
            'gpu',
            'sas',
            'unlimited'
        ]

        mem_cmd = ['-M', f'{mem}G']
        cpu_cmd = ['-n', str(cpu_num)]

        # If the queue is interactive, add -Is
        if queue in interactive:
            int_cmd = ['-Is']
        else:
            int_cmd = []

        # If the queue is gpu, add -gpu -
        # This is only the default setting, you can change it if needed.
        if 'gpu' in queue:
            gpu_cmd = ['-gpu -']
        else:
            gpu_cmd = []

        cmds = cmds + queue_cmd + mem_cmd + cpu_cmd + int_cmd + gpu_cmd
        cmds.append(script)

        return ' '.join(cmds)
    
    def run_bsub(self, mem, cpu_num, use_gpu, script, interactive=False):
        '''
        Run a bsub command on the server.

        Args:
            mem (int): The memory in GB.
            cpu_num (int): The number of CPUs.
            use_gpu (bool): Whether to use GPU.
            script (str): The script to run.

        Return:
            str: The output of the command.
        '''

        # Use GPU × Interactive
        if not interactive:
            if use_gpu:
                queue = 'gpu'
            else:
                queue = 'short'
        else:
            if use_gpu:
                queue = 'gpu_int'
            else:
                queue = 'long_int'

        command = self.get_bsub_command(queue=queue, mem=mem, cpu_num=cpu_num, script=script)
        return self.exec_command(command, pseudo_terminal=interactive)

    def bkill(self, job_id):
        '''
        Kill a job on the server. You should use it with get_bjobs().

        Args:
            job_id (str): The job id to kill.
        '''
        self.exec_command(f'bkill {job_id}')

    def kill_all(self):
        '''
        Kill all your running jobs on the server. Be careful to use it.
        '''
        bjobs_df = self.get_bjobs()
        ids = bjobs_df['JOBID'].to_list()
        for id in ids:
            self.bkill(id)

    # File Operation

    def get_sftp_cwd(self):
        '''
        Get the current working directory of the sftp connection.
        '''
        return self.sftp.getcwd()

    def set_sftp_cwd(self, path):
        '''
        Change the current working directory of the sftp connection.
        '''
        self.sftp.chdir(path)

    def is_directory(self, path):
        '''
        Check if the path is a directory. For recursive operation.
        '''
        try:
            return stat.S_ISDIR(self.sftp.stat(path).st_mode)
        except IOError:
            return False

    def download_file(self, remote_path, local_path):
        '''
        Download a file from the server.
        '''
        if not self.quiet:
            print('Downloading file from server: remote_cwd/{} to local_cwd/{}'.format(remote_path, local_path))
        self.sftp.get(remote_path, local_path)

    def upload_file(self, local_path, remote_path):
        '''
        Upload a file to the server.
        '''
        if not self.quiet:
            print('Uploading file from local_cwd/{} to server: remote_cwd/{}'.format(local_path, remote_path))
        self.sftp.put(local_path, remote_path)

    def delete_file(self, remote_path):
        '''
        Delete a file from the server.
        '''
        if not self.quiet:
            print('Deleting file from server: remote_cwd/{}'.format(remote_path))
        self.sftp.remove(remote_path)

    def delete_folder(self, remote_folder):
        '''
        Delete a folder from the server recursively.
        We choose not to use rmtree because it may have some issues.
        '''
        # Delete recursively
        print('Deleting folder from server: remote_cwd/{}'.format(remote_folder))
        for item in self.sftp.listdir(remote_folder):
            remote_item_path = f'{remote_folder}/{item}'
            if self.is_directory(remote_item_path):
                self.delete_folder(remote_item_path)
            else:
                self.delete_file(remote_item_path)
        # Delete the folder itself
        self.sftp.rmdir(remote_folder)

    def download_folder(self, remote_folder, local_folder):
        '''
        Download a folder from the server recursively.
        '''
        # Check if local folder exists
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
            print(f'Local folder does not exist. Created {local_folder}')
        else:
            print(f'Local folder exists. {local_folder}')

        # Check if remote folder exists
        if not self.is_directory(remote_folder):
            print(f'Remote folder does not exist. {remote_folder}')
            raise ValueError(f'Remote folder does not exist. {remote_folder}')
        else:
            print(f'Remote folder exists. {remote_folder}')

        # Download files
        for item in self.sftp.listdir(remote_folder):
            remote_item_path = f'{remote_folder}/{item}'
            local_item_path = f'{local_folder}/{item}'
            if self.is_directory(remote_item_path):
                self.download_folder(remote_item_path, local_item_path)
            else:
                self.download_file(remote_item_path, local_item_path)

    def upload_folder(self, local_folder, remote_folder, replace=True, delete_other=False):
        '''
        Upload a folder to the server recursively.

        Args:
            local_folder (str): The local folder to upload.
            remote_folder (str): The remote folder to upload.
            replace (bool): Whether to replace the existing files in the remote folder.
            delete_other (bool): Whether to delete the files not in the local folder.
        '''
        # Check if remote folder exists
        try:
            self.sftp.stat(remote_folder)
        except IOError:
            self.sftp.mkdir(remote_folder)
            print(f'Target folder does not exists, create: {remote_folder}')
        else:
            print(f'Target folder exists: {remote_folder}')

        # Iterate over items in the local folder
        for item in os.listdir(local_folder):
            local_item_path = os.path.join(local_folder, item)
            remote_item_path = f'{remote_folder}/{item}'

            if os.path.isdir(local_item_path):
                # If the item is a folder, recursively upload it
                self.upload_folder(local_item_path, remote_item_path, replace, delete_other)
            else:
                # If the remote file exists, decide whether to replace it
                try:
                    self.sftp.stat(remote_item_path)
                except IOError: # Not a file, but a folder
                    pass
                else:
                    if replace:
                        print(f'Remote file exists, delete: {remote_item_path}')
                        self.delete_file(remote_item_path)
                    else:
                        print(f'Remote file exists, skip: {remote_item_path}')
                        continue
                self.upload_file(local_item_path, remote_item_path)

        for item in self.sftp.listdir(remote_folder):
            local_item_path = os.path.join(local_folder, item)
            remote_item_path = f'{remote_folder}/{item}'
            if item not in os.listdir(local_folder):
                if self.is_directory(remote_item_path):
                    print(f'Remote subfolder not in local folder: {remote_item_path}')
                    self.delete_folder(remote_item_path)
                else:
                    print(f'Remote file not in local folder: {remote_item_path}')
                    self.delete_file(remote_item_path)

def run_layout(rcs: Grid, interactive=False):
    script = '/usr/local/app/rcs_bin/grid3/envs/rcs_2023.09/bin/python /export/projects4/mmiller_emrisk/ocr/layout_surya.py'
    rcs.run_bsub(mem=64, cpu_num=4, use_gpu=True, script=script, interactive=interactive)

def run_ocr_prep(rcs, job_num):
    prep_script = f'/usr/local/app/rcs_bin/grid3/envs/rcs_2023.09/bin/python /export/projects4/mmiller_emrisk/ocr/ocr_preparation.py {job_num}'
    return rcs.run_bsub(mem=16, cpu_num=16, use_gpu=False, script=prep_script, interactive=False)

def run_ocr_parallel(rcs: Grid, job_num, prep_job_id):
    ocr_cmd = f'bsub -w done({prep_job_id}) -J "OCRjobArray[1-{job_num}]" -M 24G -n 6 /usr/local/app/rcs_bin/grid3/envs/rcs_2023.09/bin/python /export/projects4/mmiller_emrisk/ocr/ocr_tesseract_jobarray.py'
    return rcs.exec_with_conda(conda_env='rcs_2023.09', script=ocr_cmd)

def run_ocr(rcs: Grid, job_num):
    prep_id = run_ocr_prep(rcs, job_num)
    ocr_id = run_ocr_parallel(rcs, job_num, prep_id)
    return ocr_id

def run_after(rcs: Grid, depends_on, script):
    return rcs.exec_command(f'bsub -w done({depends_on}) {script}')


if __name__ == '__main__':
    rcs = Grid(user_name='rcs_xchen', password='Hitorie866815166@', quiet=False)
    rcs.connect()
    rcs.open_sftp()

    # local_code_path = 'source/derived/reports/ocr'
    # target_code_path = '/export/projects4/mmiller_emrisk/ocr'
    # rcs.upload_folder(local_code_path, target_code_path, replace=True, delete_other=True)

    run_layout(rcs)
    # run_ocr(rcs, job_num=5)



    

    rcs.close()