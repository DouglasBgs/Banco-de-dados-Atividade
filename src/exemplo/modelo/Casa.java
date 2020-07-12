package exemplo.modelo; 

public class Casa {
	private int id;
	private int numero;
	private String dono;

	public Casa() { }

	public Casa(int id, String dono, int numero) {
		this.id = id;
		this.dono = dono;
		this.numero = numero;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getdono() {
		return dono;
	}

	public void setDono(String dono) {
		this.dono = dono;
	}

	public int getNumero() {
		return numero;
	}

	public void setNumero(int numero) {
		this.numero = numero;
	}

	@Override
	public String toString() {
		return "Casa [id=" + id + ", dono=" + dono + ", numero=" + numero + "]";
	}
	
	
}
