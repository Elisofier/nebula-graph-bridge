from app import nebula_graph

space = 'test'
port = '9559'
ip = '127.0.0.1'
nebula = nebula_graph.Nebula(ip, port, 'root', 'nebula', space)

_account = {
    'id': "1",
}
account = {}
for acp in _account:
    if _account[acp] is not None:
        account[acp] = _account[acp]

vid = "account?id=1"
entity = {
    'info': account,
    'tag_name': 'account',
    'vid': vid
}
nebula.update_entity(entity)
# nebula.insert_entity(entity)
