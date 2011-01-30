import os,sys, unittest, tempfile,shutil

from Rambler.RunLoop import  RunLoop, Port
from Rambler.Application import Application
from Rambler.MessageBus import IMessageObserver, MessageBus

from zope.interface import implements

from Rambler import defer
defer.setDebugging(True)

class Test(unittest.TestCase):


    def setUp(self):
        self.runLoop = RunLoop.currentRunLoop()
        self.runLoop.waitBeforeCalling(60, self.timeout)
        
        self.timedOut = False
        self.tempdir = tempfile.mkdtemp('.apps')
        self.appBundlePath = os.path.join(self.tempdir, 'testapp')
        os.mkdir(self.appBundlePath)
        
        self.appConfPath = os.path.join(self.appBundlePath, 'app.conf')
    
        self.configTemplate = '''
        <app>
        <!-- the worlds most boring app -->
        </app>
        ''' 

        open(self.appConfPath,'w').write(self.configTemplate)
    
    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def testLoad(self):
        app = Application(self.appBundlePath)
        self.failUnlessEqual(app.status, Application.STOPPED)
        assert app.status == Application.STOPPED
        def onStarted(app):
            self.failUnlessEqual(app.status, Application.STARTED)
            # this should gracefully shutdown the application and then stop our runLoop
            app.shutdown()

        d = app.load()
        d.addCallback(onStarted)
        d.addErrback(self.stopRunLoop)

        self.runLoop.run()
        self.failUnlessEqual(app.status, Application.STOPPED)



    def stopRunLoop(*args, **kw):
        if len(args) > 0 and hasattr(args[0], 'printTraceback'):
            args[0].printTraceback()
            
        # used to stop the runLoop from via a deffered object which usually calls back with one or more args
        RunLoop.currentRunLoop().stop()
    stopRunLoop = staticmethod(stopRunLoop)

    def timeout(self):
        # test took to long to complete, set a flag and stop the RunLoop
        self.timedOut = True
        RunLoop.currentRunLoop().stop()




def test_suite():
    suite = unittest.makeSuite(Test)

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')


        

