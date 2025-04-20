package com.atguigu.realtimeads;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.realtimeads.mapper")
public class RealtimeAdsApplication {

	public static void main(String[] args) {
		SpringApplication.run(RealtimeAdsApplication.class, args);
	}

}
