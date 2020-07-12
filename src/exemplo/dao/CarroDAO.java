package exemplo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import exemplo.modelo.Carro;
import exemplo.modelo.Departamento;

public class CarroDAO implements IDao<Carro> {

	public CarroDAO() {
		try {
			createTable();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void createTable() throws SQLException {
		final String sqlCreate = "IF NOT EXISTS (" 
				+ "SELECT * FROM sys.tables t JOIN sys.schemas s ON " 
				+ "(t.schema_id = s.schema_id) WHERE s.name = 'dbo'" 
				+ "AND t.name = 'Carro')"
				+ "CREATE TABLE Carro"
				+ " (id	int	IDENTITY,"
				+ "  proprietario	VARCHAR(255),"
				+ "	 placa	VARCHAR(10),"
				+ "  PRIMARY KEY (id))";
		
		Connection conn = DatabaseAccess.getConnection();
		
		Statement stmt = conn.createStatement();
		stmt.execute(sqlCreate);
	}

	public List<CarroDAO> getAll() {
		Connection conn = DatabaseAccess.getConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		List<CarroDAO> Carros = new ArrayList<CarroDAO>();
		
		try {
			stmt = conn.createStatement();
			
			String SQL = "SELECT * FROM Carro";
	        rs = stmt.executeQuery(SQL);

	        while (rs.next()) {
	        	CarroDAO d = getCarroFromRs(rs);
	        	
	        	Carros.add(d);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getAllCarros] Erro ao selecionar todos os carros", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return Carros;		
	}
	
	public CarroDAO getById(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		CarroDAO Carro = null;
		
		try {
			String SQL = "SELECT * FROM Carro WHERE id = ?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);

	        rs = stmt.executeQuery();
	        
	        while (rs.next()) {
	        	Carro = getCarroFromRs(rs);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getCarroById] Erro ao selecionar o carro por id", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return Carro;		
	}
	
	public void insert(CarroDAO Carro) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String SQL = "INSERT INTO Carro (proprietario, placa) VALUES (?, ?)";
			stmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
	    	stmt.setString(1, Carro.getProprietario());			
	        stmt.executeUpdate();
	        
	        rs = stmt.getGeneratedKeys();
	        
	        if (rs.next()) {
	        	Carro.setId(rs.getInt(1));
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[insereCarro] Erro ao inserir o Carro", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	public void delete(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
			
		try {
			String SQL = "DELETE Carro WHERE id=?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        stmt.executeUpdate(); 			
		} catch (SQLException e) {
			throw new RuntimeException("[deleteCarro] Erro ao remover o Carro por id", e);
		} finally {
			close(conn, stmt, null);
		}
	}
	
	public void update(CarroDAO Carro) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "UPDATE Carro SET proprietario = ?, placa = ? WHERE id=?";
			stmt = conn.prepareStatement(SQL);
	    	stmt.setString(1, Carro.getProprietario());
	    	stmt.setString(2, Carro.getPlaca());
	    	stmt.setInt(3, Carro.getId());
	    	
	        stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("[updateCarro] Erro ao atualizar o Carro", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	private CarroDAO getCarroFromRs(ResultSet rs) throws SQLException {
		CarroDAO d = new CarroDAO();
		d.setId(rs.getInt("id"));
		d.setProprietario(rs.getString("proprietario"));
		d.setPlaca(rs.getString("placa"));
		
		return d;
	}
	
	private void close(Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null) { rs.close(); }
			if (stmt != null) { stmt.close(); }
			if (conn != null) { conn.close(); }
		} catch (SQLException e) {
			throw new RuntimeException("Erro ao fechar recursos.", e);
		}
	}

}
