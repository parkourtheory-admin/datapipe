'''
Video processing module

Author: Justin Chen
Date: 5/11/2020

'''

import cv2

class Format(object):
    '''
    inputs:
    h   (int) Output video height
    w   (int) Output video width
    fps (int) Output video frames per second
    '''
    def __init__(self, h, w, fps=None):
        self.h = h
        self.w = w
        self.fps = fps


    '''
    Writes resize video as mp4

    inputs:
    filename (str) Input file path
    output   (str) Output file path
    '''
    def resize(self, filename, output):
        dout = (self.h, self.w)
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
