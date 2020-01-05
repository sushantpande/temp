from multiprocessing.dummy import Pool as ThreadPool

class Worker(object):
    def __init__(self, threads):
        self.pool = ThreadPool(processes=threads)

    def do_work(self, task, on_error=None):
        def do_task():
            try:
                return task()
            except Exception as ex:
                if on_error:
                    self.pool.apply_async(on_error, args=(ex,))

        return self.pool.apply_async(do_task)
