import click
from metameq import write_extended_metadata as _write_extended_metadata


@click.group()
def root():
    pass


@root.command("write-extended-metadata",
              context_settings={'show_default': True})
@click.argument('metadata_file_path', type=click.Path(exists=True))
#                help='path to the metadata file to be extended')
@click.argument('config_fp', type=click.Path(exists=True))
#                help='path to the study-specific config yaml file')
@click.argument('name_base', type=str)
#                help='base name for the output extended metadata file')
@click.option('--out_dir', default=".",
              help='output directory for the extended metadata file')
@click.option('--sep', default="\t",
              help='separator of input file (default is tab); '
                   'not applicable to excel files')
@click.option('--suppress_fails_files', is_flag=True,
              help='suppress output of QC and validation error files if no'
                   'errors found.  Default is to output empty files.')
def write_extended_metadata(metadata_file_path, config_fp,
                            out_dir, name_base, sep, suppress_fails_files):
    _write_extended_metadata(
        metadata_file_path, config_fp, out_dir, name_base,
        sep, suppress_empty_fails=suppress_fails_files)


if __name__ == '__main__':
    root()
