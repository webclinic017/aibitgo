## HelloWorld
> 数据库  

## 运行条件
> 搭建数据库并创建用户


	create user 'hello'@'localhost' identified by '123456';

	grant all privileges on *.* to 'hello'@'localhost'  WITH GRANT OPTION;

	CREATE DATABASE hello_world CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;



>创建数据库：  
    drop database hello_world;  
    CREATE DATABASE hello_world CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
    
>提交数据库版本：  
    
    alembic revision --autogenerate -m "init" 

>更新数据库结构：
    
    开发环境 alembic -c alembic_test.ini upgrade head
    生产环境 alembic upgrade head
     
>回退数据库结构：

    开发环境 alembic -c alembic_test.ini downgrade 版本号
    生产环境 alembic downgrade 版本号
    
    
