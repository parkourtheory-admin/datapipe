'''
Video processing module

Author: Justin Chen
Date: 5/11/2020

'''

import cv2
import os
import io
import base64
from tqdm import tqdm
from PIL import Image
from more_itertools import chunked
from multiprocessing import Process, Manager, cpu_count

class Video(object):
    '''
    inputs:
    
    fps (int) Output video frames per second
    '''
    def __init__(self, fps=None):
        self.fps = fps


    '''
    Writes resize video as mp4

    inputs:
    height   (int)  Output video height
    width    (int)  Output video width
    filename (str)  Input file path
    output   (str)  Output file path
    res      (dict) Process Manager dictionary
    '''
    def resize(self, height, width, filename, output, res):
        dout = (height, width)
        out_file = f'{output}.mp4' if not output.endswith('.mp4') else output
        cap = cv2.VideoCapture(filename)

        if self.fps == None:
            self.fps = int(cap.get(cv2.CAP_PROP_FPS))

        out = cv2.VideoWriter(out_file, 0x7634706d, self.fps, dout)

        count = 0

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret: break
            
            b = cv2.resize(frame, dout, fx=0, fy=0, interpolation=cv2.INTER_CUBIC)
            out.write(b)
            count += 1

        cap.release()
        out.release()

        f = filename.split('/')[-1]
        res[f] = count == 0


    '''
    Generate thumbnails using the middle frame.

    inputs:
    src    (str) Absolute path to src video
    height (int, optional) Thumbnail height. Default: 300
    width  (int, optional) Thumbnail width. Default: 168

    outputs:
    failed (bool) True if successfully saved thumbnail
    '''
    def thumbnail(self, res, src, height=300, width=168):
        vidcap = cv2.VideoCapture(src)
        
        _, image = vidcap.read()
        _, buffer = cv2.imencode('.jpg', cv2.resize(image,(height, width)))
        
        embed = src.split('/')[-1]
        res[embed] = f'data:image/png;base64, {base64.b64encode(buffer)}'
        
        vidcap.release()


    '''
    Extract thumbnails in parallel. Assumes the file names are already formatted so that 
    the results dictionary can be used to align with and to update the video table.


    inputs:
    src    (str) Source directory containing videos
    height (int) Crop height
    width  (int) Crop width

    outputs:
    res (dict) Dictionary with file name as key and serialized thumbnail as value
    '''
    def extract_thumbnails(self, src, height, width):
        files = [i for i in os.listdir(src)]
        cpus = cpu_count()
        mgmt = Manager()
        res = mgmt.dict()

        for block in tqdm(chunked(iter(files), cpus), total=len(files)//cpus+1):
            procs = []

            for f in block:
                procs.append(Process(target=self.thumbnail, 
                    args=(res, os.path.join(src, f), height, width)))
            
            for p in procs: p.start()
            for p in procs: p.join()
        print(dict(res).keys())
        return res