package com.sample.java.notification;

public class Client {

	public static void main(String[] args) {
		StateHolder stateHolder = new StateHolder();
		EmailListener email1 = new EmailListener();
		SMSListener sms1 = new SMSListener();
		
		stateHolder.addStateListener(email1);
		stateHolder.setState(10);//checks the listener Set and broadcast, if any change
		
		stateHolder.setState(20);
		
		stateHolder.addStateListener(email1);
		stateHolder.addStateListener(sms1);
		stateHolder.setState(30);
		
		//New way
		StateHolderAdv stateHolderAdv = new StateHolderAdv();
		stateHolderAdv.addStateListener(email1);
		stateHolderAdv.setState(10);//checks the listener Set and broadcast, if any change
		
		stateHolderAdv.setState(20);
		
		stateHolderAdv.addStateListener(email1);
		stateHolderAdv.addStateListener(sms1);
		stateHolderAdv.setState(30);
		
		
	}

}
