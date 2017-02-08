#!usr/bin/env python
# -*- coding=utf-8 -*-

import os
import socket
import io
import shutil
import time
from smb.SMBConnection import SMBConnection
from smb.smb_structs import OperationFailure


class RetrieveStatus(object):
    # Status class to record
    # TODO: How to better implement
    ###############################  NOT IN USE NOW

    def __init__(self, status, code, comment=""):
        self.status = dict(status=status, status_code=code, comment=comment)

    def __call__(self, **kwargs):
        self.status = self.status.update(kwargs)


def get_host_name_by_ip(ip):
    try:
        hostname, aliaslist, ipaddrlist = socket.gethostbyaddr(ip)
    except Exception, err:
        return {"comment": "Get Hostname fail!"}
    return {"hostname": hostname}


def build_connection(user, password, ip, my_name, computer_name, port=139):
    conn = SMBConnection(user, password, my_name, computer_name)
    assert conn.connect(ip, 139)
    return conn


def test_smb_connection(conn, send_msg="test connection"):
    # Test the smb connection with SMBConnection echo method
    # Send an echo command containing data to the remote SMB/CIFS server. The remote SMB/CIFS will reply with the same data.
    try:    
        reply_msg = conn.echo(send_msg)
        assert send_msg == reply_msg
    except Exception:
        return False
    else:
        return True


def smb_directory_tree(conn, share_name, path="\\", output_file=None):
    
    # current & parent directory
    CUR_AND_PAR_DIR = (".", "..")

    root = unicode(share_name)
    tree = dict(root=dict(path=share_name, 
                          nondirs=list(),
                          dirs=list()))

    def check_dir(smb_obj):

        if (getattr(smb_obj, "filename", None) not in CUR_AND_PAR_DIR and getattr(smb_obj, "isDirectory", None)):
            return smb_obj.filename
        return False

    def walk(path, search=55, pattern='*', timeout=30):

        smb_list = conn.listPath(share_name, path, search, pattern, timeout)
        dirs, nondirs = list(), list()

        for smb_obj in smb_list:
            if not check_dir(smb_obj) and smb_obj.filename not in CUR_AND_PAR_DIR:
                nondirs.append(smb_obj.filename)
            elif check_dir(smb_obj):
                dirs.append(walk(os.path.join(path, smb_obj.filename), search, pattern, timeout))
            else:
                pass
        return dict(path=path, nondirs=sorted(nondirs), dirs=sorted(dirs, key= lambda x: x['path']))

    # def map_tree(tree, output_file):

    #     def draw_nondirs(nondirs, level, lines=[]):
    #         for item in nondirs:
    #             lines.append(" " * 4 * level + u"·" + item + "\n")
    #         return lines

    #     def draw_dirs(directory, level, lines=[]):
    #         lines.append(" " * 4 * level + "+ " + unicode(directory.get("path")) + "\n")
    #         nondirs = directory.get("nondirs")
    #         next_dirs = directory.get("dirs")
    #         if nondirs:
    #             lines += draw_nondirs(nondirs, level+1, lines)
    #         if next_dirs:
    #             for d in next_dirs:
    #                 lines += draw_dirs(d, level+1, lines)
    #         return lines

    #     with io.open(output_file, "w", encoding="utf-8") as f:
            

    tree = walk(path)
    # if output_file:
    #     map_tree(tree, output_file)
    return tree


def retrieve_file(conn, share_name, path, save_path, replace=False, timeout=30):
    # Return retrieve result with a dictionary
    # 
    ################################################
    #############   Result Description:   ##########
    # status   ###   status_code     ###  comment    
    # Fail     ###       01          ###  Error occu when retrieving file
    # Fail     ###       02          ###  Connection Lost；SMBConnection timeout or break
    # Fail     ###       03          ###  KeyboardInterrupt when retrieving file
    # Fail     ###       04          ###  File not accessible for reading
    # Success  ###       11          ###  Successfully retrieved from server
    # Abort    ###       21          ###  file with same name exists in the saving path

    def retrieve(conn, share_name, path, save_path, replace, timeout):
        filename = os.path.basename(path)
        save_file = os.path.join(save_path, filename)
        if os.path.exists(save_file) and not replace:
            return {"status_code": "21", "status": "Abort", "comment": "File with same name already exists!"}
        try:
            with io.open(save_file, "wb") as s_f:
                print ("Retrieving......")
                f = conn.retrieveFile(share_name, path, s_f, timeout)
        except Exception, err:
            if os.path.exists(save_file):
                os.remove(save_file)
            return {"status_code": "01", "status": "Fail", "comment": err}
        except KeyboardInterrupt:
            return {"status_code": "03", "status": "Fail", "comment": "KeyboardInterrupt, stopped mannually!"}
        except OperationFailure:
            return {"status_code": "04", "status": "Fail", "comment": "File cannot opened for reading!"}
        else:
            return {"status_code": "11", "status": "Sucess", "comment": "File successfully retrieved and saved!"}

    retrieve_result = retrieve(conn, share_name, path, save_path, replace, timeout)
    return retrieve_result


def retry_retrieve_file(conn, share_name, path, save_path, replace=False, timeout=30, interval=5, retry_time=0):

    def retry_retrieve(conn, share_name, path, save_path, replace, timeout, interval):
        if not test_smb_connection(conn):
            return {"status_code": "02", "status": "Fail", "comment": "Connnection Lost!"}
        print ("{0} try fail! another retry will start in {1}s!".format(retry_time, interval))
        time.sleep(interval)
        return retrieve_file(conn, share_name, path, save_path, replace, timeout)

    if not retry_time:
        while True:
            retrieve_result = retry_retrieve(conn, share_name, path, save_path, replace, timeout, interval)
            if retrieve_result.get("status_code") == "04":
                continue
            if not retrieve_result.get("status_code").startswith("0"):
                return retrieve_result
    for retry_time in range(1, timeout):
        retrieve_result = retry_retrieve(conn, share_name, path, save_path, replace, timeout, interval)
        if not retrieve_result["status_code"].startswith("0"):
            return retrieve_result


def retrieve_file_from_tree(conn, tree, share_name, save_path, replace=False, 
                            timeout=30, interval=5, retry_time=0):

    path = tree.get("path")
    nondirs = tree.get("nondirs")
    dirs = tree.get("dirs")

    aim_path = os.path.join(save_path, *path.split(os.sep)[1:])
    if not os.path.exists(aim_path):
        os.mkdir(aim_path)

    for item in nondirs:
        retrieve_result = retrieve_file(conn, share_name, os.path.join(path, item),
                                        aim_path, replace, timeout)
        if retrieve_result.get("status_code") == "02":
            retry_retrieve_file(conn, share_name, path, aim_path, replace, timeout, interval, retry_time)

    if dirs:
        for d in dirs:
            retrieve_file_from_tree(conn, d, share_name, save_path, replace, timeout, interval, retry_time)


def main():

    IP = "192.168.1.190"
    USER = "shareman"
    PASSWORD = "______"
    SHARE_NAME = "share"
    MY_NAME = "test01"
    COMPUTER_NAME = get_host_name_by_ip(IP).get("hostname")

    ######################
    path = "\\"
    save_path = "F:\\tmp_dir\\"
    ######################

    if not COMPUTER_NAME:
        return {"status_code": "02", "status":"Fail", "comment": "Get hostname failed!"}
    conn = build_connection(USER, PASSWORD, IP, MY_NAME, COMPUTER_NAME)
    tree = smb_directory_tree(conn, SHARE_NAME, path=path)

    retrieve_file_from_tree(conn, tree, SHARE_NAME, save_path, replace=False, 
                            timeout=30, interval=5, retry_time=0)


if __name__ == '__main__':
    main()