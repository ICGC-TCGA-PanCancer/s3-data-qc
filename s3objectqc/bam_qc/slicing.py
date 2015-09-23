from ..job_tracker import move_to_next_step, get_job_json, save_job_json

name = 'slicing'
next_step = None

def get_name():
    global name
    return name


def run(conf, job_json_file):
    print ('running task: {}'.format(get_name()))

    # many steps here

    # if everything was fine, finally move the job json file to the success folder
    move_to_next_step(conf, name, 'match', job_json_file)

