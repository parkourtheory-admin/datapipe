'''
Video processing module

Author: Justin Chen
Date: 5/11/2020

'''

import cv2
import os

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
    Generate thumbnails

    inputs:
    src    (str) Absolute path to src video
    dst    (str) Absolute path to save dir
    height (int, optional) Thumbnail height. Default: 300
    width  (int, optional) Thumbnail width. Default: 168

    outputs:
    failed (bool) True if successfully saved thumbnail
    '''
    def thumbnail(self, src, dst, height=300, width=168):
        vidcap = cv2.VideoCapture(src)
        success,image = vidcap.read()
        count = 0
        mid = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))//2
        filename = src.split('/')[-1].split('.')[0]

        while success:
            count += 1

            if count == mid:
                image = cv2.resize(image,(height, width))
                cv2.imwrite(os.path.join(dst, f'{filename}.jpg'), image)     # save frame as JPEG file

                if success: return True

            success, image = vidcap.read()

        return False


if __name__ == '__main__':
    src = '/media/ch3njus/Seagate4TB/research/parkourtheory/data/videos/production/'
    dst = './thumbnails'
    v = Video()
    files = [i for i in os.listdir(src)]
    failed = []

    for file in tqdm(iter(files), total=len(files)):
        success = v.thumbnail(os.path.join(src,file), dst, 300, 168)
        
        if not success:
            failed.append(file)

    print(failed)
