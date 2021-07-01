from db.db_context import session_socpe
from db.model import ExchangeAPIModel, UserModel, GroupModel, GroupUserRelationModel
from util.hash_util import HashUtil


def init_data():
    try:
        with session_socpe() as sc:
            api = {'id': '1', "api_key": "479411e2-b48f-41d5-8d35-704e3191b659",
                   "secret_key": "56E578C84FD6D68A66E0A90BFE847243", "passphrase": "123456", 'account': 'aibitgo2108',
                   'exchange': 'okex'}
            api = ExchangeAPIModel(**api)
            sc.merge(api)

            # add user
            user = UserModel(
                id=1,
                username="aibitgo",
                password=HashUtil.md5("aibitgo2108$&"),
                phone=10086,
                email="10000@qq.com"
            )
            sc.merge(user)
            print('添加用户1')
            user2 = UserModel(
                id=2,
                username="aibitgo2108",
                password=HashUtil.md5("aibitgo21082108"),
                phone=2222,
                email="2222@qq.com"
            )
            print(HashUtil.md5("aibitgo2"))
            print('添加用户2')
            sc.merge(user2)
            # add user
            user3 = UserModel(
                id=3,
                username="Kevin",
                password=HashUtil.md5("aibitgo2108"),
                phone=10086,
                email="100002@qq.com"
            )
            sc.merge(user3)

            group = GroupModel(
                id=1,
                name="admin"
            )
            sc.merge(group)
            group2 = GroupModel(
                id=2,
                name="super_admin"
            )
            sc.merge(group2)
            print('添加组')
            relation = GroupUserRelationModel(
                user_id=user.id,
                group_id=group.id
            )
            sc.merge(relation)
            relation2 = GroupUserRelationModel(
                user_id=user2.id,
                group_id=group2.id
            )
            sc.merge(relation2)
        print("初始数据成功")
    except Exception as e:
        print("添加初始数据失败:\n {reason}".format(reason=str(e)))


if __name__ == '__main__':
    init_data()
