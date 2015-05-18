# NAnt.Custom.Tasks
RetryServiceController task for managing WIN32 services

#How to use
##By including "retryservicecontroller.include"
```
<code>
<?xml version="1.0" encoding="utf-8" ?>
<project name="NAnt.Custom.Tasks" default="stop" basedir="." xmlns="http://nant.sourceforge.net/release/0.85/nant.xsd" >
    <include buildfile="retryservicecontroller.include"/>
    <target name="start">
        <retryservicecontroller action="Start" service="Adobe LM Service" machine="SFIRSOV" timeout="5000" retryCount="3"/>
    </target>
</project>
</code>
```
##By adding NAnt.Custom.Tasks.Dll
You can place the compiled version of NAnt.Custom.Tasks.DLL into the nant installation folder and start using retryservicecontroller task right after it without any additional changes.
