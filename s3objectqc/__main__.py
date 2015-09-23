import sys
from config import get_config
from job import Job


# TODO: add logging support

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    config_file = args[0] if args else 'config.yaml'  # default config file

    my_config = get_config(config_file)

    while True:
    	# get next job
    	job = Job(my_config)
        if job.runable:
            job.run()
        else:
            break

    # better logging to be added
    print('\nNo job or unable to fetch job in the queue, stop now.\n')


if __name__ == "__main__":
    sys.exit(main())
