'''
Data pipeline API
'''
import traceback
import multiprocessing as mp

'''
Parallel task execution

inputs:
pipe (list) Tasks to execute
'''
def parallel(pipe):
    pipe = [mp.Process(target=t.run) for t in pipe]
    
    for t in pipe: t.start()
    for t in pipe: t.join()

'''
Sequential execution of pipeline

inputs:
pipe (list) Tasks to execute
log  (dict) Log for failed tasks
'''
def sequential(pipe, log, verbose=True):
    for i, t in enumerate(pipe):
        task = type(t).__name__

        try: 
            t.run()
            print(f'[{i}] {task} succeeded\n')
        except Exception as e:
            tb = traceback.format_exc()
            err = tb if verbose else str(e)
            
            print(f'[{i}] {task}.py failed\n\n{err}\n')

            log[type(t).__name__] = tb


'''
Check that pipe is unique

inputs:
pipe (list) List of tasks

output:
pipe (set) Unique set of tasks
'''
def unique(pipe):
	p = set(pipe)
	assert len(p) == len(pipe)
	return p