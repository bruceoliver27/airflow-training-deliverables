import os
import collections
import inspect
import yaml
from das42.utils.utils import Utils
from datetime import timedelta, datetime


class JobConfig(Utils):
    def __init__(self):
        super().__init__()
    @staticmethod
    def get_config(config_path=None):
        dir_name = os.path.dirname(__file__)
        if config_path is None:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            filename = os.path.splitext(os.path.basename(module.__file__))[0]
            config_path = os.path.join(os.path.join(
                dir_name,
                "..",
                "config",
                filename + ".yaml"
            ))
        env_args = JobConfig._load_file(os.path.join(
            dir_name,
            "..",
            "config/env.yaml"
        ))
        global_args = JobConfig._load_file(os.path.join(
            dir_name,
            "..",
            "config/globals.yaml"
        ))
        try:
            global_args["default_args"]["retry_delay"] = timedelta(
                seconds=global_args["default_args"]["retry_delay"]
            )
        except Exception:
            pass
        try:
            global_args["default_args"]["max_retry_delay"] = timedelta(
                seconds=global_args["default_args"]["max_retry_delay"]
            )
        except Exception:
            pass
        try:
            global_args["default_args"]["start_date"] = \
                datetime.strptime(
                    global_args["default_args"]["start_date"],
                    "%Y-%m-%d %H:%M"
                    )
        except Exception:
            pass
        job_args = JobConfig._load_file(config_path)
        try:
            job_args["default_args"]["retry_delay"] = timedelta(
                seconds=job_args["default_args"]["retry_delay"]
            )
        except Exception:
            pass
        try:
            job_args["default_args"]["max_retry_delay"] = timedelta(
                seconds=job_args["default_args"]["max_retry_delay"]
            )
        except Exception:
            pass
        try:
            job_args["default_args"]["start_date"] = \
                datetime.strptime(
                    job_args["default_args"]["start_date"],
                    "%Y-%m-%d %H:%M"
                    )
        except Exception:
            pass
        JobConfig._dict_merge(global_args, job_args)
        JobConfig._dict_merge(global_args, env_args)
        return global_args
    @staticmethod
    def _load_file(path=None):
        with open(path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return config
    @staticmethod
    def _dict_merge(dct, merge_dct):
        """
        Recursive dict merge. Inspired by :meth:``dict.update()``, instead
        of updating only top-level keys, dict_merge recurses down into dicts
        nested to an arbitrary depth, updating keys. The ``merge_dct`` is
        merged into ``dct``.
        :param dct: dict onto which the merge is executed
        :param merge_dct: dct merged into dct
        :return: None
        """
        for k, v in merge_dct.items():
            if (k in dct and isinstance(dct[k], dict)
                    and isinstance(merge_dct[k], collections.Mapping)):
                JobConfig._dict_merge(dct[k], merge_dct[k])
            else:
                dct[k] = merge_dct[k]
