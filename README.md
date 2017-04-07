# 项目描述
 
 stormcv-实时视频流处理系统
 
 

# 项目开发流程 

## 分支 (Branch)
* 一般master分支默认是被锁住，其目的是保护该分支。
* 1.普通开发人员需创建issue后建立对应的分支然后去完成任务。 创建分支名建议： git branch jkyan/AddOfflineScheduler ； （用户名/需求描述）
* 2.完成issue后便要合并分支，只需发送merge request ，等待owner审核通过才能合并到master分支上。
* 3.合并的过程中可能会出现代码冲突问题。而这个问题却交给Master去处理，因为普通开发人员是没有权限的 。
* 4.master在合并分支时的方法：
    * 在window上安装TortoiseGit
    * 首先在windows下将当前待合并分支的代码clone下来。 git clone -b [branch name] [repo url]  [local dir name]
    * 然后选择TortoiseGit的merge选项，选择将远端的master分支合并到当前分支中。 remotes/origin/master
    * 合并过程可能会出现冲突，点击Resolve解决冲突即可。Their即为master的代码，Mine为当前分支的代码；
    * 冲突解决后commit并且push到远端。这时候网页端已经不会再显示有冲突，在网页上merge就可以。


## 问题(Issue)

issue的用途：
- 缺陷：需要进行修正的地方。
- 功能：需要实现的很酷很棒的新点子。
- 待完成清单：待完成的检查清单。

##  issue与Merge Request的关联：

当你提交一个commit的时候在commit message里面使用#issue, 比如issuer description #8 , github就会自动关联issue 8跟这个commit. 当然在github上面写comment的时候使用这个也是有效的.

那么如何跟随着commit关闭一个issue呢? 在confirm merge的时候可以使用一下命令来关闭相关issue:
```
fixes #xxx
fixed #xxx
fix #xxx
closes #xxx
close #xxx
closed #xxx
```

## 开发流程
1. 本地配置好git开发环境，添加本地ssh key到gitlab；
2. 将代码clone到本地。git clone命令；
3. 在本地建立开发分支。 命令： git checkout -b jkyan/AddFaceDetect
4. 在开发分支上修改代码。（stormcv是maven项目，可以使用eclipse或者idea导入）
5. 本地修改add, commit并且push到远程仓库。 push命令： git push origin jkyan/AddFaceDetect
6. push完之后可以在10.134.143.51网页上看到让你创建merge request的请求；
9. 按照提示创建merge request即可。（如果当前分支修改内容有对应的issue，按照上面介绍的关联并关闭issue）
10. 如果代码修改与master分支没有冲突，可以直接合入到master（需要管理员权限）；
11. 如果代码修改与master分支有冲突，按照前面提到的解决冲突的办法，在windows下使用TortoiseGit软件来解决冲突；


