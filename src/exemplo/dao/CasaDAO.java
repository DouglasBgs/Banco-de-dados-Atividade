package exemplo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import exemplo.modelo.Casa;
import exemplo.modelo.Departamento;

public class CasaDAO implements IDao<Casa> {

	public CasaDAO() {
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
				+ "AND t.name = 'Casa')"
				+ "CREATE TABLE Casa"
				+ " (id	int	IDENTITY,"
				+ "  dono	VARCHAR(255),"
				+ "	 numero	int,"
				+ "  PRIMARY KEY (id))";
		
		Connection conn = DatabaseAccess.getConnection();
		
		Statement stmt = conn.createStatement();
		stmt.execute(sqlCreate);
	}

	public List<CasaDAO> getAll() {
		Connection conn = DatabaseAccess.getConnection();
		Statement stmt = null;
		ResultSet rs = null;

		List<CasaDAO> Casas = new ArrayList<CasaDAO>();

		try {
			stmt = conn.createStatement();

			String SQL = "SELECT * FROM Casa";
	        rs = stmt.executeQuery(SQL);

	        while (rs.next()) {
	        	CasaDAO d = getCasaFromRs(rs);
	        	
	        	Casas.add(d);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getAllCasas] Erro ao selecionar todos os Casas", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return Casas;		
	}
	
	public CasaDAO getById(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		CasaDAO Casa = null;
		
		try {
			String SQL = "SELECT * FROM Casa WHERE id = ?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);

	        rs = stmt.executeQuery();
	        
	        while (rs.next()) {
	        	Casa = getCasaFromRs(rs);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getCasaById] Erro ao selecionar a casa por id", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return Casa;		
	}
	
	public void insert(CasaDAO Casa) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String SQL = "INSERT INTO Casa (dono, numero) VALUES (?, ?)";
			stmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
	    	stmt.setString(1, Casa.getDono());
	    	stmt.setInt(2, Casa.getNumero());
	    	
			
	        stmt.executeUpdate();
	        
	        rs = stmt.getGeneratedKeys();
	        
	        if (rs.next()) {
	        	Casa.setId(rs.getInt(1));
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[insereCasa] Erro ao inserir a Casa", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	public void delete(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
			
		try {
			String SQL = "DELETE Casa WHERE id = ?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        stmt.executeUpdate(); 			
		} catch (SQLException e) {
			throw new RuntimeException("[deleteCasa] Erro ao remover a Casa por id", e);
		} finally {
			close(conn, stmt, null);
		}
	}
	
	public void update(CasaDAO Casa) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "UPDATE Casa SET dono = ?, numero = ? WHERE id=?";
			stmt = conn.prepareStatement(SQL);
	    	stmt.setString(1, Casa.getDono());
	    	stmt.setInt(2, Casa.getNumero());
	    	stmt.setInt(3, Casa.getId());
	    	
	        stmt.executeUpdate(); // executa o UPDATE			
		} catch (SQLException e) {
			throw new RuntimeException("[updateCasa] Erro ao atualizar a casa", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	private CasaDAO getCasaFromRs(ResultSet rs) throws SQLException {
		CasaDAO d = new CasaDAO();
		d.setId(rs.getInt("id"));
		d.setDono(rs.getString("dono"));
		d.setNumero(rs.getString("numero"));
		
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
