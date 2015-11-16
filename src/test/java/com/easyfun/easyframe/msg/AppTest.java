package com.easyfun.easyframe.msg;

public class AppTest {
	public static void main(String[] args) {
		TestRun t1= new TestRun("test1");
		TestRun t2= new TestRun("test1");
		t1.start();
		t2.start();
	}

	private static class TestRun extends Thread {
		private String name;

		public TestRun(String name) {
			this.name = name;
		}

		public void run() {
			MsgIterator iter = MsgUtil.consume("applog", "linzmGroup");
			while (iter.hasNext()) {
				System.out.println(name + ": " + iter.next());
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
