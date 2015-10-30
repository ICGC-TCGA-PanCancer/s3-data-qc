import sys
from config import get_config


# TODO: add logging support

def load_job_class(job_type):
    try:
        job_module = __import__(job_type.lower() + '.job', globals(), locals(), [], -1)
        job_class = getattr(job_module, job_type.upper())
    except Exception as e:
        sys.exit('Unable to load specified job type: {}. Error: {}!\n'\
            .format(job_type, e.message))

    return job_class


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    config_file = args[0] if args else 'config.yaml'  # default config file
    run_dir = args[1] if len(args) > 1 else ''  # support specifying run_dir / run_id

    my_config = get_config(config_file, run_dir)

    job_class = load_job_class(my_config.get('job_type'))

    while True:
    	# get next job
        print('\nFetching new jobs...\n')
        job = job_class(my_config)
        if job.runable:
            job.run()
        elif not my_config.get('keep_running_even_queue_empty'):
            break

    # better logging to be added
    print('\nNo job or unable to fetch job in the queue, stop now.\n')


if __name__ == "__main__":
    sys.exit(main())
