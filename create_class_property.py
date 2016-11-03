#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
生成类中私有变量的 getter 和 setter 方法
注意: 私有变量是以 单个下划线(_) 开头的, 并非 双下划线(__) 开头
"""

class BackupBase(object):
    _cmd_path = None # yes
    _db_host = None # yes
    _db_port = None # yes
    _db_username = None # yes
    _db_password = None # yes
    _local_dir = None # yes
    _remote_dir = None # yes

    __aa = None # no
    __bb__ = None # no

    def _a(): # no
        pass

def create_getter_setter(property):
    func_str = '''
    @property
    def {property}(self):
        """{property} 是一个属性 - getter 方法"""
        return self._{property}

    @{property}.setter
    def {property}(self, value):
        """{property}属性的 setter 方法"""
        self._{property} = value
    '''.format(property = property)
    print func_str


def main():
    import inspect
    # import BackupBase # 导入需要生成 getter 和 setter 方法的类

    obj = BackupBase() # 实例化对象
    obj_class_name = obj.__class__.__name__ # 获得实例的类的名称

    for name, value in inspect.getmembers(obj): # 循环类的属性
        # 判断是否是私有变量
        if (name.startswith('_') and not name.startswith('__')
           and not name.startswith('_{class_name}'.format(class_name=obj_class_name))
           and not inspect.isroutine(value)):

            property = name.lstrip('_')
            create_getter_setter(property)


if __name__ == '__main__':
    main()
