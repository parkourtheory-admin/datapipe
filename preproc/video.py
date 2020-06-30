'''
Video processing module

Author: Justin Chen
Date: 5/11/2020

'''

import cv2
import os
import io
import base64
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
    height   (int) Output video height
    width    (int) Output video width
    filename (str) Input file path
    output   (str) Output file path
    '''
    def resize(self, height, width, filename, output):
        dout = (height, width)
        out_file = f'{output}.mp4' if not output.endswith('.mp4') else output
        cap = cv2.VideoCapture(filename)
        # fourcc = cv2.VideoWriter_fourcc(*'MP4V')

        if self.fps == None:
            self.fps = cap.get(cv2.CAP_PROP_FPS)

        out = cv2.VideoWriter(out_file, 0x7634706d, self.fps, dout)

        while 1:
            ret, frame = cap.read()
            if not ret: break

            b = cv2.resize(frame, dout, fx=0, fy=0, interpolation=cv2.INTER_CUBIC)
            out.write(b)

        cap.release()
        out.release()


    '''
    Generate thumbnails using the middle frame.

    inputs:
    src    (str) Absolute path to src video
    dst    (str) Absolute path to save dir
    height (int, optional) Thumbnail height. Default: 300
    width  (int, optional) Thumbnail width. Default: 168
    save   (bool, optional) Save image to disk. If False, returns the image as a base64 string

    outputs:
    failed (bool) True if successfully saved thumbnail
    '''
    def thumbnail(self, res, src, dst, height=300, width=168, save=False):
        vidcap = cv2.VideoCapture(src)
        success,image = vidcap.read()
        count = 0
        mid = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))//2
        filename = src.split('/')[-1].split('.')[0]

        while success:
            count += 1

            if count == mid:
                image = cv2.resize(image,(height, width))
                                    
                if save:
                    # save frame as JPEG file
                    cv2.imwrite(os.path.join(dst, f'{filename}.jpg'), image)
                else:
                    _, buffer = cv2.imencode('.jpg', image)
                    res[filename] = f'data:image/png;base64, {base64.b64encode(buffer)}'
                    vidcap.release()
                    break

                vidcap.release()

            success, image = vidcap.read()


    '''
    Extract thumbnails in parallel

    inputs:
    src (str) Source directory containing videos
    dst (str) Destination directory for thumbnails
    '''
    def extract_thumbnails(self, src, dst):
        files = [i for i in os.listdir(src)]
        cpus = cpu_count()
        mgmt = Manager()
        res = mgmt.dict()

        for block in chunked(iter(files), cpus):
            procs = []

            for f in block:
                procs.append(Process(target=self.thumbnail, 
                    args=(res, os.path.join(src, f), dst, 300, 168)))
            
            for p in procs: p.start()
            for p in procs: p.join()

        return res.values()