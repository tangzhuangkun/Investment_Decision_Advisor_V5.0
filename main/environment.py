# 决定使用哪个环境
import platform


class Envirnment:
    # 获取运行环境信息
    # 频率：每次运行都会判断环境
    # Mac 即 Darwin系统为开发环境，dev
    # Linux, 为运行的生产环境，pro

    def __init__(self):
        # 给环境打标签
        self.tag_envirment = {'Darwin':'dev', 'xx':'test', 'Linux':'pro' }

    def tag_envirnment(self):
        # 获取系统信息并打标
        sys = platform.system()
        env =  self.tag_envirment[sys]
        return env



if __name__ == '__main__':
    go = Envirnment()
    print(go.tag_envirnment())


#env = 'dev'
#env = 'test'		
#env = 'pro'
