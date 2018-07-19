package com.cg.app.util;

import com.cg.app.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class TransactionDecoder implements Decoder<User> {

	
	private static final ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public User fromBytes(byte[] bytes) {
		try {
			return mapper.readValue(bytes, User.class);
			
		}catch(Exception e) {
			e.printStackTrace();
			
			
		}
		return null;
	}

	public TransactionDecoder() {
		
	}
	
	public TransactionDecoder(VerifiableProperties props) {
		
	}
	
	
	

}
