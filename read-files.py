import os, argparse, io
import re
import xml.etree.ElementTree as ET
from contextlib import closing
from multiprocessing import Pool
import pickle

matching_file_re = re.compile('^(calls|sms).+\.xml(\.xz)?$', re.I)


def open_xz(filepath):
    try:
        import lzma
        return lzma.open(filepath)
    except ImportError:
        import subprocess
        args = ['/usr/local/bin/xz', '-d', '--stdout', filepath]
        output = subprocess.check_output(args)
        return io.BytesIO(output)


def node_to_dict(node):
    node_dict = dict(node.items(), _tag=node.tag)
    if node.text:
        node_dict['_text'] = node.text
    kids = node.getchildren()
    if kids:
        node_dict['_children'] = [node_to_dict(node) for node in kids]
    return node_dict


def parse_file(filepath):
    print('parsing: %s' % filepath)
    if filepath.endswith('.xz'):
        fp = open_xz(filepath)
    else:
        fp = open(filepath)
    try:
        with closing(fp):
            tree = ET.parse(fp).getroot()
    except ET.ParseError as pe:
        print('Unable to read %s: %s' % (filepath, pe))
        return

    for node in tree:
        yield node_to_dict(node)


def parse_file_list(filepath):
    return list(parse_file(filepath))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dir')
    ap.add_argument('-p', '--pickle', default='all.pickle')
    args = ap.parse_args()
    files = list(sorted(set(find_matching_files(args.dir))))
    nodes = []
    with Pool(8) as p:
        for p_nodes in p.map(parse_file_list, files):
            nodes.extend(p_nodes)
    print('writing %s' % args.pickle)
    with open(args.pickle, 'wb') as outf:
        pickle.dump(nodes, outf, pickle.HIGHEST_PROTOCOL)


def find_matching_files(dir):
    for path, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            if not matching_file_re.match(filename):
                continue
            filepath = os.path.join(path, filename)
            yield filepath


if __name__ == '__main__':
    main()
