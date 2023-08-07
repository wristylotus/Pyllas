import sys


class ProgressBar:

    def __init__(self, prefix='Running: ', filler='█',
                 complete='Successfully completed.',
                 error='Completed with failure!'):
        self.progress = ''
        self.prefix = prefix
        self.filler = filler
        self.complete = complete
        self.error = error

    def __enter__(self):
        return self

    def tick(self):
        self.progress += self.filler
        sys.stdout.write(f'\r{self.prefix}{self.progress}')
        sys.stdout.flush()

    def __exit__(self, _type, value, traceback):
        sys.stdout.write(f'\n{self.complete if not traceback else self.error}\n')
        sys.stdout.flush()
