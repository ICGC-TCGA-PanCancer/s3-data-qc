import sys
from config import get_config


# TODO: add logging support

def load_job_class(job_type):
    try:
        job_module = __import__(job_type.lower() + '.job', globals(), locals(), [], -1)
        job_class = getattr(job_module, job_type.upper())
    except:
        sys.exit('Unable to load specified job type: {}!\n'.format(job_type))

    return job_class


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    config_file = args[0] if args else 'config.yaml'  # default config file

    my_config = get_config(config_file)
    
    job_class = load_job_class(my_config.get('job_type'))

    while True:
    	# get next job
        job = job_class(my_config)
        if job.runable:
            job.run()
        else:
            break

    # better logging to be added
    print('\nNo job or unable to fetch job in the queue, stop now.\n')


if __name__ == "__main__":
    sys.exit(main())
