import re
from base64 import b64encode


def create_insert_entity(entity: dict, vid_type):
    vid = entity['vid']
    vid = str(b64encode(vid.encode(encoding='utf-8'))).replace("\'", "")
    if len(vid) > 100:
        vid = vid[0:100]

    tag_list = ''
    value_list = ''
    info = entity['info']
    for k in info:
        if info[k] is None:
            pass
        elif type(info[k]) == str:
            ki = entity['info'][k]
            ki = ki.replace('\\', "\\\\")
            ki = ki.replace('\'', "\\'")
            ki = ki.replace('\"', "\\'")
            ki = ki.replace('\n', '\\n')
            tag_list = tag_list + k + ', '
            value_list = value_list + '\"' + ki + '\", '
        else:
            tag_list = tag_list + k + ', '
            value_list = value_list + entity['info'][k] + ', '
    tag_list = tag_list[0:-2]
    value_list = value_list[0:-2]
    if vid_type == 'str':
        gql = 'insert vertex if not exists ' + entity[
            'tag_name'] + '(' + tag_list + ') values ' + '\"' + vid + '\":( ' + value_list + ')'
    else:
        gql = 'insert vertex if not exists ' + entity[
            'tag_name'] + '(' + tag_list + ') values ' + vid + ':( ' + value_list + ')'
    return gql, vid


def create_get_entity_by_tag_and_vid(tag, vid: str, vid_type):
    vid = str(b64encode(vid.encode(encoding='utf-8'))).replace("\'", "")
    if len(vid) > 100:
        vid = vid[0:100]

    if vid_type == 'str':
        gql = 'FETCH PROP ON ' + tag + ' \'' + vid + '\'' + ' YIELD properties(vertex)'
    else:
        gql = 'FETCH PROP ON ' + tag + ' ' + vid + ' YIELD properties(vertex)'
    return gql


def create_get_entity_by_tag_and_query(tag: str, query: list):
    gql = []
    for q in query:
        ql = ""
        for k in q:
            ql += tag + "." + str(k) + " == '" + str(q[k]) + "' AND "
        ql = ql[:-4]
        gql.append("LOOKUP ON " + tag + " WHERE " + ql + "yield vertex as vertex_")
    return gql


def create_insert_edge(vid_type, edge: dict):
    src_vid = edge['src_vid']
    src_vid = str(b64encode(src_vid.encode(encoding='utf-8'))).replace("\'", "")
    if len(src_vid) > 100:
        src_vid = src_vid[0:100]
    dst_vid = edge['dst_vid']
    dst_vid = str(b64encode(dst_vid.encode(encoding='utf-8'))).replace("\'", "")
    if len(dst_vid) > 100:
        dst_vid = dst_vid[0:100]

    edge_list = ''
    value_list = ''
    info = edge['info']
    for k in info:
        if type(info[k]) == str:
            edge_list = edge_list + k + ', '
            value_list = value_list + '\'' + edge['info'][k] + '\', '
        else:
            edge_list = edge_list + k + ', '
            value_list = value_list + edge['info'][k] + ', '
    edge_list = edge_list[0:-2]
    value_list = value_list[0:-2]
    if vid_type == 'str':
        gql = 'INSERT EDGE IF NOT EXISTS ' + edge[
            'edge_name'] + '(' + edge_list + ') VALUES \'' + src_vid + '\'->\'' + dst_vid + '\': (' + value_list + ');'
    else:
        gql = 'INSERT EDGE IF NOT EXISTS ' + edge[
            'edge_name'] + '(' + edge_list + ') VALUES ' + ': (' + value_list + ');'
    return gql


def create_insert_edge_bulk(edge: list, vid_type: str):
    gql = ''
    return gql


def create_update_entity(entity, vid_type):
    vid = entity['vid']
    vid = str(b64encode(vid.encode(encoding='utf-8'))).replace("\'", "")
    if len(vid) > 100:
        vid = vid[0:100]

    tag_list = ''
    value_list = ''
    info = entity['info']
    for k in info:
        if info[k] is None:
            pass
        elif type(info[k]) == str:
            ki = entity['info'][k]
            ki = ki.replace('\\', "\\\\")
            ki = ki.replace('\'', "\\'")
            ki = ki.replace('\"', "\\'")
            ki = ki.replace('\n', '\\n')
            tag_list = tag_list + k + ', '
            value_list = value_list + '\"' + ki + '\", '
        else:
            tag_list = tag_list + k + ', '
            value_list = value_list + entity['info'][k] + ', '
    tag_list = tag_list[0:-2]
    value_list = value_list[0:-2]
    if vid_type == 'str':
        gql = 'insert vertex ' + entity[
            'tag_name'] + '(' + tag_list + ') values ' + '\"' + vid + '\":( ' + value_list + ')'
    else:
        gql = 'insert vertex ' + entity[
            'tag_name'] + '(' + tag_list + ') values ' + vid + ':( ' + value_list + ')'
    return gql, vid
