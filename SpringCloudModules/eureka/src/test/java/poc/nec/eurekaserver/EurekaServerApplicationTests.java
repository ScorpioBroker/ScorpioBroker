package poc.nec.eurekaserver;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
public class EurekaServerApplicationTests {

	@Test
	public void contextLoads() {
	}

}
