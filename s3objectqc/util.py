import hashlib
import subprocess

def get_md5(fname, use_shell):
    if use_shell:
        process = subprocess.Popen(
                'md5sum {}'.format(fname),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()

        if process.returncode:
            # should not exit for just this error, improve it later
            print('Unable to run \'md5sum\' tool.\nError message: {}'.format(err))
            return None
        else:
            return out.split()[0]

    else:
        hash = hashlib.md5()
        with open(fname) as f:
            for chunk in iter(lambda: f.read(1024*256), ""):
                hash.update(chunk)
        return hash.hexdigest()
