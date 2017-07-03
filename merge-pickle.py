import hashlib
import json
import pickle
import argparse
import dateparser


def hash_dict(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode('utf-8')).hexdigest()


def deduplicate_nodes(nodes):
    print('%d nodes before deduplication' % len(nodes))
    node_set = {}
    for node in nodes:
        h = hash_dict(node)
        node_set[h] = node
    print('%d nodes (%.2f%%) after deduplication' % (len(node_set), len(node_set) / len(nodes) * 100))
    nodes = list(node_set.values())
    return nodes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-i', '--input', dest='input', default='all.pickle')
    ap.add_argument('-d', '--dedupe', default=False, action='store_true')
    ap.add_argument('-p', '--output-pickle', dest='output_pickle', default=None)
    ap.add_argument('-j', '--output-json', dest='output_json', default='data.json')
    args = ap.parse_args()
    print('Reading %s' % args.input)
    with open(args.input, 'rb') as infp:
        nodes = pickle.load(infp)
    if args.dedupe:
        nodes = deduplicate_nodes(nodes)
        if args.output_pickle:
            print('Writing deduplicated pickle: %s' % args.output_pickle)
            with open(args.output_pickle, 'wb') as outfp:
                pickle.dump(nodes, outfp, protocol=pickle.HIGHEST_PROTOCOL)
    print('Infusing %d nodes with timestamps' % len(nodes))
    for node in nodes:
        node['_ts'] = dateparser.parse(node['readable_date']).timestamp()
    print('Sorting %d nodes' % len(nodes))
    nodes.sort(key=lambda n: (n['_ts'], n['_tag']))
    print('Writing JSON to %s' % args.output_json)
    with open(args.output_json, 'w', encoding='UTF-8') as outfp:
        json.dump(nodes, outfp, indent=2)


if __name__ == '__main__':
    main()
