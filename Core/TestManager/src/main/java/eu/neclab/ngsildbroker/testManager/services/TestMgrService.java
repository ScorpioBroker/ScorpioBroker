package eu.neclab.ngsildbroker.testManager.services;


//@Service
public class TestMgrService {

	//TODOpublic class Greeting {

    private final long id;
    private final String content;

    public TestMgrService(long id, String content) {
        this.id = id;
        this.content = content;
    }

    public long getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

}
