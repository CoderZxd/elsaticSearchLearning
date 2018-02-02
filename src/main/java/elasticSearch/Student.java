/**
 * 
 */
package elasticSearch;

import java.util.Date;

import com.alibaba.fastjson.JSON;

/**
 * @author Admin
 *
 */
public class Student {
	private Long id;
	private String ename;
	private String name;
	private String sexy;//male,female
	private String brithday;
	private String info;
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getEname() {
		return ename;
	}

	public void setEname(String ename) {
		this.ename = ename;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSexy() {
		return sexy;
	}

	public void setSexy(String sexy) {
		this.sexy = sexy;
	}

	public String getBrithday() {
		return brithday;
	}

	public void setBrithday(String brithday) {
		this.brithday = brithday;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	@Override
	public String toString(){
		return JSON.toJSONString(this);
	}
}
