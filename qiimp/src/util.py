import copy
import os
import yaml


def extract_config_dict(config_fp, starting_fp=None):
    if config_fp is None:
        grandparent_dir = _get_grandparent_dir(starting_fp)
        config_fp = os.path.join(grandparent_dir, "config.yml")

    # read in config file
    config_dict = extract_yaml_dict(config_fp)
    return config_dict


def _get_grandparent_dir(starting_fp=None):
    if starting_fp is None:
        starting_fp = __file__
    curr_dir = os.path.dirname(os.path.abspath(starting_fp))
    grandparent_dir = os.path.join(curr_dir, os.pardir, os.pardir)
    return grandparent_dir


def extract_yaml_dict(yaml_fp):
    with open(yaml_fp, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def extract_stds_config(stds_fp):
    if not stds_fp:
        stds_fp = os.path.join(_get_grandparent_dir(), "standards.yml")
    return extract_config_dict(stds_fp)


def deepcopy_dict(input_dict):
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = deepcopy_dict(curr_val)
        else:
            output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict
