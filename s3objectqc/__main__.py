import sys
from config import get_config
from start_job import start_next_job
from download_job import download_job
from slice_job import slice_job

# TODO: add logging support

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    config_file = args[0] if args else 'config.yaml'  # default config file

    my_config = get_config(config_file)

    while True:
    	# get next job
    	job = start_next_job(my_config)

    	# stop when job is empty
        if not job: sys.exit('No job or unable to fetch job in the queue, stop now.')
        
        # download bam
    	job = download_job(job, my_config)

        # slice bam (only needed when download_job qc produces matched result)
        if job: slice_job(job, my_config)


if __name__ == "__main__":
    main()
