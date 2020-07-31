"""Some helper functions for interacting with bash terminal."""

import subprocess


def subprocess_cmd(command, DEBUG=False):
    """execute a bash command and return output"""
    if DEBUG:
        print("command: {}".format(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    if DEBUG:
        print("out: {}".format(proc_stdout))
    return(proc_stdout)
