## HelloWorld
> 量化交易  

## 运行条件
>生成requirements.txt文件：  

    pip freeze > requirements.txt
    
>安装requirements.txt依赖：  

    pip install -r requirements.txt
    
>提交数据库版本：  
    
    alembic revision --autogenerate -m "init" 

>更新数据库结构：
    
    开发环境 alembic upgrade head
    生产环境 alembic -c production.ini upgrade head
     
>回退数据库结构：

    开发环境 alembic downgrade 版本号
    生产环境 alembic -c production.ini downgrade 版本号
    
>运行所有测试  
    
    python -m pytest -vvs test/
    
>运行单个测试  
    
    python -m pytest -vvs test/test_cache.py
    
>启动web服务 方法1
    
    sh start_web.sh
    
>启动web服务 方法2

    uvicorn web.web_api:app --reload 
    
>增加PYTHONPATH
    
    export PYTHONPATH=$(pwd)
    
>启动交易服务器

    python main.py execution
   
>启动基差机器人

    python main.py basis 机器人ID
