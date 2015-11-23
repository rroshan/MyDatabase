
public class Record {
	private int id;
	private String company;
	private String drug_id;
	private short trials;
	private short patients;
	private short dosage_mg;
	private float reading;
	private boolean deleted = false;
	private boolean double_blind = false;
	private boolean controlled_study = false;
	private boolean govt_funded = false;
	private boolean fda_approved = false;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	public String getDrug_id() {
		return drug_id;
	}
	public void setDrug_id(String drug_id) {
		this.drug_id = drug_id;
	}
	public short getTrials() {
		return trials;
	}
	public void setTrials(short trials) {
		this.trials = trials;
	}
	public short getPatients() {
		return patients;
	}
	public void setPatients(short patients) {
		this.patients = patients;
	}
	public short getDosage_mg() {
		return dosage_mg;
	}
	public void setDosage_mg(short dosage_mg) {
		this.dosage_mg = dosage_mg;
	}
	public float getReading() {
		return reading;
	}
	public void setReading(float reading) {
		this.reading = reading;
	}
	public boolean isDeleted() {
		return deleted;
	}
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
	public boolean isDouble_blind() {
		return double_blind;
	}
	public void setDouble_blind(boolean double_blind) {
		this.double_blind = double_blind;
	}
	public boolean isControlled_study() {
		return controlled_study;
	}
	public void setControlled_study(boolean controlled_study) {
		this.controlled_study = controlled_study;
	}
	public boolean isGovt_funded() {
		return govt_funded;
	}
	public void setGovt_funded(boolean govt_funded) {
		this.govt_funded = govt_funded;
	}
	public boolean isFda_approved() {
		return fda_approved;
	}
	public void setFda_approved(boolean fda_approved) {
		this.fda_approved = fda_approved;
	}
}
