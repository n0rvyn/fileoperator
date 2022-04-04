#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2020-2022 by ZHANG ZHIJIE.
# All rights reserved.

# Last Modified Time: 4/2/22 19:59
# Author: ZHANG ZHIJIE
# Email: norvyn@norvyn.com
# Git: @n0rvyn
# File Name: fileoperator.py
# Tools: PyCharm

"""
---Record time of different method removing a directory which contains large number of small files---

"""

import os
import sys
import time
import subprocess
import threading
import random

_HOME_ = os.path.abspath(os.path.dirname(__file__))
_TARGET_DIR_PATH_ = 'test_delete'
_EMPTY_DIR_PATH_ = 'empty'

_TEST_DIR_PREFIX_ = 'test'

_DEL_METHODS_ = [
    # Warning: NEVER define a command ends with ' '(blank)
    f'''rsync -a --delete-before -d {_EMPTY_DIR_PATH_}/ {_TEST_DIR_PREFIX_}1/''',
    f'''rsync -a --delete {_EMPTY_DIR_PATH_}/ {_TEST_DIR_PREFIX_}2/''',
    f'''rm -rf {_TEST_DIR_PREFIX_}3''',
    f'''find {_TEST_DIR_PREFIX_}4 -maxdepth 1 -type f -delete''',
    f'''find {_TEST_DIR_PREFIX_}5 -type f -delete''',
    f'''find {_HOME_} -maxdepth 1 -type d -name {_TEST_DIR_PREFIX_}6 -exec rm -rf {{}} \;''',
    # find -type d -delete --> only effective to empty directory
    f'''find {_HOME_} -type d -name {_TEST_DIR_PREFIX_}7 -exec rm -rf {{}} \;'''
]


class ProcessConsole(object):
    def __init__(self):
        pass

    @staticmethod
    def get_pid(command: str) -> int:
        def _trans_special_ch(_command: str) -> str:
            for ch in ['{', '}', '\\']:
                _command = _command.replace(ch, f'''\\{ch}''')
            return _command

        command = _trans_special_ch(command).strip()
        _pid_command = subprocess.getoutput('which pgrep')
        # print('Checking path of pgrep: {:>15s}'.format(_pid_command))
        if not _pid_command.startswith('/'):
            print('ERROR: command "pgrep" not found!')
            return 0
        try:
            _pid = int(subprocess.getoutput(f'''{_pid_command} -nf "{command}"'''))
        except ValueError:
            return 0
        return _pid

    @staticmethod
    def pid_cpu_mem_usage(pid: int):
        """
        return: tuple(pid, %cpu, %mem)
        """
        return tuple(subprocess.getoutput(f'''ps -eo pid,pcpu,pmem | grep -w "{pid}"''').split()) if 0 != pid else (
        0, 0, 0)

    def cal_backend_command_lifetime_usage(self, command: str):
        def _target(_command):
            os.system(f'{_command} >/dev/null 2>&1')

        t = threading.Thread(target=_target, args=(command,))
        t.start()
        _start_time = time.perf_counter()

        _max_cpu = _max_mem = _all_cpu = _all_mem = 0
        _pid = self.get_pid(command)

        _interval = 1
        _count = 0
        while t.is_alive():
            try:
                _, _cpu, _mem = self.pid_cpu_mem_usage(_pid)
            except ValueError:
                break

            try:
                _pid = int(_pid)
                _cpu = float(_cpu)
                _mem = float(_mem)
            except ValueError as _e:
                print(_e)
                return False

            _max_cpu = _cpu if _cpu >= _max_cpu else _max_cpu
            _max_mem = _mem if _mem >= _max_mem else _max_mem

            _all_cpu += _cpu
            _all_mem += _mem

            time.sleep(_interval)
            _count += 1

        # todo division by _lifetime or '_count * _interval', it's a question?
        try:
            _avg_cpu = _all_cpu / _count / _interval
            _avg_mem = _all_mem / _count / _interval
        except ZeroDivisionError:
            _avg_mem = _avg_cpu = 0

        # t.join()
        _lifetime = time.perf_counter() - _start_time

        _return = f"""PID: {str(_pid): >5s}| MAX CPU/MEM: {_max_cpu:6.2f}/{_max_mem:5.2f}| AVG CPU/MEM: {_avg_cpu:6.2f}/{_avg_mem:5.2f}| TOTAL CPU/MEM: {_all_cpu:7.2f}/{_all_mem:5.2f}|TIME: {_lifetime:7.2f}"""
        return _return


class FileOperator(object):
    def __init__(self):
        pass

    @staticmethod
    def trans_file_size(file_size) -> int:
        """
        return: file size in byte
        """
        try:
            # check if file_size is just int or float
            # then Unit will be 'byte'
            file_size = int(file_size)
            return file_size

        except ValueError:
            # file_size ends with Unit

            # trans file_size to upper case
            file_size = file_size.upper()

            # strip letter 'b'
            file_size = file_size.strip('b')
            try:
                _size = int(file_size[0:-1])
            except ValueError as _e:
                raise _e

            if 'K' in file_size:
                return _size * 1024
            if 'M' in file_size:
                return _size * 1024 * 1024
            if 'G' in file_size:
                return _size * 1024 * 1024 * 1024
            if 'T' in file_size:
                return _size * 1024 * 1024 * 1024

    def gen_file(self, file_size, target_dir=None, file_name=None, size_limit=None):
        """
        Generate large number of small files to one target directory

        By default, a warning will be given if file_size is larger than size_limit(4M)

        """
        target_dir = '.' if target_dir is None else target_dir
        file_name = str(random.randint(10e9, 10e10)) if file_name is None else file_name
        size_limit = 4 * 1024 * 1024 if size_limit is None else self.trans_file_size(size_limit)

        file_size = self.trans_file_size(file_size)

        # if file been created is larger than 'size_limit', raise warning.
        if file_size >= size_limit:
            prompt = input(f'\nFile size larger than {size_limit} Byte, make sure you really want to do this: (No/yes)')
            if prompt.upper() not in ['Y', 'YES']:
                print('Nothing happened!\n')
                return False
        file_path = os.path.join(os.path.realpath(target_dir), file_name)

        with open(file_path, 'wb') as f:
            f.seek(file_size - 1)
            f.write(b'\0')

        return True if os.path.isfile(file_name) else False

    def gen_files(self, file_size, no_files: int, target_dir=None, file_name=None, size_limit=None):
        def _gen_file_target(_size, _dir, _name, _limit):
            self.gen_file(file_size=_size, target_dir=_dir, file_name=_name, size_limit=_limit)

        threads = [threading.Thread(target=_gen_file_target, args=(file_size, target_dir, file_name, size_limit),
                                    name=f'thread_{i}') for i in range(no_files)]

        [t.start() for t in threads]
        [t.join() for t in threads]

    def gen_files_multi_dirs(self, file_size, no_files: int, target_dirs=None,
                             file_name=None,
                             size_limit=None,
                             target_dirs_prefix=None,
                             no_target_dirs=None,
                             silent=False):
        """
        return: float -> time spent to generate those directories and files
        """
        target_dirs_suffix = '' if target_dirs_prefix is None else target_dirs_prefix
        no_target_dirs = 0 if no_target_dirs is None else no_target_dirs
        target_dirs = target_dirs if target_dirs is not None else [f'{target_dirs_suffix}{i + 1}' for i in
                                                                   range(no_target_dirs)]

        try:
            pwd_free = int(subprocess.getoutput('df -m `pwd`').split()[-3])
        except ValueError as _e:
            raise _e
        allocated_space = self.trans_file_size(file_size) * no_files * len(target_dirs) // 1024 // 1024
        print(f'About \033[0;31m{allocated_space}\033[0m MiB files will be created, '
              f'current free space of this block is \033[0;31m{pwd_free}\033[0m MiB.\n')
        if pwd_free <= allocated_space:
            print(f'Error: no enough space on this device.')
            return 0

        def _gen_files_one_dir_target(_size, _no, _dir, _name, _limit):
            self.gen_files(file_size=_size, no_files=_no, target_dir=_dir, file_name=_name, size_limit=_limit)

        # verity if the directory in 'target_dirs' exist or not
        # 1. exist, prompt to rename, transfer to absolutely path
        # 2. not exist, join _HOME_ with the relpath
        # finally, create directory with real absolutely path.
        def _raise_prompt(_name):
            if not silent and os.path.exists(_name):
                _prompt = input(
                    f'The directory name specified {_name} already exist, rename to {_name}.auto (Yes/no): ')
                if _prompt.upper() in ['N', 'NO']:
                    return os.path.join(_HOME_, _name)
                _name = os.path.join(_HOME_, f'{_name}.auto')
            _name = os.path.join(_HOME_, _name)
            try:
                os.mkdir(_name)
            except FileExistsError as __e:
                print('INFO: ', __e) if not silent else ''
            return _name

        target_dirs = [_raise_prompt(_dir) for _dir in target_dirs]
        target_dirs_lines = '\n  '.join(target_dirs)

        if not silent:
            prompt = input(f'\nDirectories: \n'
                           f'  {target_dirs_lines}\n'
                           f'will be filled with [{no_files}] files as size of [{file_size}] (No/yes): ')
            if prompt.upper() not in ['Y', 'YES']:
                return 0

        threads = [threading.Thread(target=_gen_files_one_dir_target,
                                    args=(file_size, no_files, target_dir, file_name, size_limit)) for target_dir in
                   target_dirs]

        start_time = time.perf_counter()
        [t.start() for t in threads]
        [t.join() for t in threads]
        return '{:.2f}'.format(time.perf_counter() - start_time)


def del_dir_perf_test(file_size, no_files, target_dirs_prefix=None, no_target_dirs=None, silent=False):
    target_dirs_prefix = _TEST_DIR_PREFIX_ if target_dirs_prefix is None else target_dirs_prefix
    no_target_dirs = 7 if no_target_dirs is None else no_target_dirs

    file_op = FileOperator()
    print(f'\nPrepare {no_target_dirs} dirs and each dir contains {no_files} files for testing: ',
          file_op.gen_files_multi_dirs(file_size, no_files,
                                       target_dirs_prefix=target_dirs_prefix,
                                       no_target_dirs=no_target_dirs,
                                       silent=silent), '(s)')

    _proc_consl = ProcessConsole()
    if not os.path.isdir(_EMPTY_DIR_PATH_):
        os.mkdir(_EMPTY_DIR_PATH_)
    for _del_command in _DEL_METHODS_:
        print('——' * 52)
        print(_proc_consl.cal_backend_command_lifetime_usage(_del_command), _del_command, sep='| ')
    print('——' * 52)

    print('\nEnd of test, check directories state after deleting by command "du -sh test*".')
    os.system('du -sh test*')


_FILENAME_ = os.path.relpath(__file__)
USAGE = f"""
Usage:
    ./{_FILENAME_} --create-dirs-only --no-files 4 --no-dirs 1 --file-size 4K --dir-prefix test [--silent]
    ./{_FILENAME_} --test-remove-dirs --no-files 80000 --file-size 4K --silent
"""

if __name__ == '__main__':
    # proc_consl = ProcessConsole()
    # print(proc_consl.cal_command_lifetime_usage('sleep 2'))

    parameters_supt = ['--create-dirs-only', '--test-remove-dirs',
                       '--no-files',
                       '--no-dirs',
                       '--file-size',
                       '--dir-prefix']
    parameters = sys.argv
    if 1 == len(parameters):
        print(USAGE)
        exit(1)

    create_dirs_only = test_remove_dirs = False

    __no_files = __no_dirs = 0
    __file_size = None

    try:
        __dir_prefix = parameters[parameters.index('--dir-prefix') + 1]
    except ValueError:
        __dir_prefix = 'test'

    try:
        _ = parameters[parameters.index('--silent')]
        __silent = True
    except ValueError:
        __silent = False

    try:
        __no_files = (parameters[parameters.index('--no-files') + 1])
        __file_size = parameters[parameters.index('--file-size') + 1]

        try:
            __no_files = int(__no_files)
        except ValueError:
            print('Number of files MUST be integer!')
            exit(126)

        try:
            parameters.index('--create-dirs-only')  # command line contains '--create-dirs-only'
            create_dirs_only = True
            test_remove_dirs = False
            try:
                # --create-dirs-only MUST be following with '--no-files' and '--no-dirs'
                __no_dirs = int(parameters[parameters.index('--no-dirs') + 1])

                file_opera = FileOperator()
                _time_spent = file_opera.gen_files_multi_dirs(__file_size, __no_files,
                                                              no_target_dirs=__no_dirs,
                                                              target_dirs_prefix=__dir_prefix,
                                                              silent=__silent)
                print(f'Time Spent: {_time_spent} (s)')
            except ValueError:
                print('Number of dirs is not integer or never specified!')
                exit(128)

        except ValueError:
            try:
                parameters.index('--test-remove-dirs')  # command line contains '--test-remove-dirs'
                test_remove_dirs = True
                create_dirs_only = False
                __no_dirs = 7

                del_dir_perf_test(__file_size, __no_files, silent=__silent)
            except ValueError:
                pass

    except ValueError:  # parameters.index('--file-size') | value not in list
        print('''Both "--no-files" and "--file-size" MUST be following with the script.''')
        exit(124)
    except IndexError:  # value of --no-files --file-size not specified.
        print('File number and size MUST be specified!')
        exit(125)
