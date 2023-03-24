from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import utils


class Nebula:
    def __init__(self, ip, port, user_name, password, space):
        self.edges = []
        self.entities = []
        config = Config()
        config.max_connection_pool_size = 10
        connection_pool = ConnectionPool()
        ok = connection_pool.init([(ip, port)], config)
        assert ok, "连接初始化失败"
        self.session = connection_pool.get_session(user_name, password)
        self.session.execute('USE ' + space)
        print("connect to " + space)
        result = self.session.execute('SHOW TAGS')
        print("tags: " + str(result))
        result = self.session.execute('SHOW EDGES')
        print("edges: " + str(result))
        self.vid_type = 'str'

    def __del__(self):
        self.session.release()

    def insert_entity(self, entity: dict):
        gql, vid = utils.create_insert_entity(entity, self.vid_type)
        # print(gql)
        resp = self.session.execute(gql)
        assert resp.is_succeeded(), resp.error_msg()
        return resp, vid

    def get_entity_by_tag_and_vid(self, tag: str, vid: str):
        gql = utils.create_get_entity_by_tag_and_vid(tag, vid, self.vid_type)
        # print(gql)
        resp = self.session.execute(gql)
        assert resp.is_succeeded(), resp.error_msg()
        res = utils.pares_data(resp)
        if len(res) == 0:
            return None
        else:
            res[0][0]['vid'] = vid
            return res[0][0]

    def get_entity_by_tag_and_query(self, tag: str, query: list):
        gql = utils.create_get_entity_by_tag_and_query(tag, query)
        # print(gql)
        res_entity = []
        for g in gql:
            resp = self.session.execute(g)
            assert resp.is_succeeded(), resp.error_msg()
            res = utils.pares_data(resp)
            if len(res) == 0:
                pass
            else:
                res_entity.append(res[0][0])
        return res_entity

    # def insert_edge(self, edge:dict):
    #
    # def get_entity_all_children(self, tag, vid):
    #
    # def get_entity_all_parents(self, tag, vid):
    #
    # def delete_entity_by_tag_and_vid(self, tag, vid):
    #
    # def delete_edge(self, tag, src_vid, dst_vid):
    #
    # def update_entity(self, entity: dict):

    def update_entity(self, entity):
        gql, vid = utils.create_update_entity(entity, self.vid_type)
        # print(gql)
        resp = self.session.execute(gql)
        assert resp.is_succeeded(), resp.error_msg()
        return resp, vid

    # def upsert_entity(self, entity):
    #     gql, vid = utils.create_update_entity(entity, self.vid_type)
    #     print(gql)
    #     resp = self.session.execute(gql)
    #     assert resp.is_succeeded(), resp.error_msg()
    #     return resp, vid

    # def delete_entity(self, entity):
        # gql, vid = utils.create_update_entity(entity, self.vid_type)
        # # print(gql)
        # resp = self.session.execute(gql)
        # assert resp.is_succeeded(), resp.error_msg()
        # return resp, vid

    # def delete_edge(self, entity):
        # gql, vid = utils.create_edit_entity(entity, self.vid_type)
        # # print(gql)
        # resp = self.session.execute(gql)
        # assert resp.is_succeeded(), resp.error_msg()
        # return resp, vid

    def insert_edge(self, edge: dict):
        gql = utils.create_insert_edge(edge=edge, vid_type=self.vid_type)
        resp = self.session.execute(gql)
        assert resp.is_succeeded(), resp.error_msg()
        return resp

    def insert_edge_bulk(self):
        gql = utils.create_insert_edge_bulk(edge=self.edges, vid_type=self.vid_type)
        resp = self.session.execute(gql)
        assert resp.is_succeeded(), resp.error_msg()
        return resp
