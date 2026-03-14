# Crypto Tax

## Project Overview

**Crypto Tax** is a full-stack web application designed to help users process cryptocurrency transactions, manage exchange keys, import trading history, and calculate crypto-related taxes. It includes features to ingest data from CSV files and external services like BloxTax, Kraken, and Binance.

**GitHub Repository:** [https://github.com/DewHead/Crypto-Tax.git](https://github.com/DewHead/Crypto-Tax.git)

### Architecture
The project is structured as a monorepo containing a Python backend and a React/Next.js frontend.

*   **Backend (`/backend`)**: A RESTful API built with **Python** and **FastAPI**. It handles business logic, database operations (via **SQLAlchemy** and **Alembic** for migrations), and data ingestion. It uses **ccxt** for cryptocurrency exchange integrations and **pandas** for data processing. The database appears to be SQLite (`ledger.db`).
*   **Frontend (`/frontend`)**: A modern web client built with **Next.js 16** (App Router), **React 19**, and **TypeScript**. For styling and UI components, it utilizes **Tailwind CSS v4** and **shadcn/ui**. Data fetching and state management are handled by **@tanstack/react-query**, while complex tables use **@tanstack/react-table**.

## Building and Running

### Prerequisites
*   **Python** (for the backend)
*   **Node.js** and **npm** (for the frontend)

### Quick Start
To start both the frontend and backend servers simultaneously for local development, you can use the provided bash script at the root of the project:

```bash
./dev.sh
```
This script will start the backend on `http://localhost:8000` and the frontend on `http://localhost:3000`.

### Running Components Individually

**Backend:**
1. Navigate to the backend directory: `cd backend`
2. Activate the virtual environment (assuming it's already created): `source venv/bin/activate`
3. Run the development server: `uvicorn main:app --reload --port 8000 --host 0.0.0.0`

**Frontend:**
1. Navigate to the frontend directory: `cd frontend`
2. Install dependencies (if not already done): `npm install`
3. Start the development server: `npm run dev`

## Development Conventions

### Backend
*   **Structure**: The backend follows a standard FastAPI project structure with `/app/api`, `/app/core`, `/app/db`, `/app/models`, `/app/schemas`, and `/app/services`.
*   **Testing**: Tests are located in the `backend/tests/` directory and use **pytest**. To run the tests, execute `pytest` from the backend directory.
*   **Database Migrations**: Alembic is used for database migrations (located in `backend/alembic/`).

### Frontend
*   **Styling**: Use Tailwind CSS utility classes and `shadcn/ui` components for building interfaces.
*   **Linting**: The project uses ESLint. Run `npm run lint` to check for code style issues.
*   **Testing**: End-to-end (E2E) testing is configured using **Playwright** (located in `frontend/e2e/`). Run E2E tests using `npx playwright test`.

### Standalone Scripts
The root directory contains several utility scripts for specific operations, such as:
*   `sync_all.py`: Likely used to synchronize transaction data.
*   `recalculate_taxes.py`: Script to trigger tax recalculations.
*   `get_2025_kpi.py`: Script to generate or retrieve KPIs for the year 2025.
*   `import_binance_csv.py`: Utility to parse and import Binance transaction CSVs.
*   `ingest_bloxtax.py`: Utility to ingest data from BloxTax.

## Available MCPs and Agent Skills

This workspace is configured with specialized capabilities tailored for modern web development and cryptocurrency tax calculation.

### Model Context Protocols (MCPs)
*   **GitHub MCP**: Provides a comprehensive suite of tools (`mcp_github_*`) to interact with GitHub directly from the CLI. This allows for reading/writing repository code, managing issues and pull requests, and pushing commits to the `Crypto-Tax` repository seamlessly.
*   **SQLite MCP**: Offers database interaction tools (`mcp_sqlite_*`) enabling direct querying, schema inspection, and data manipulation of the `ledger.db` SQLite database used by the backend.

### Agent Skills
The following AI agent skills are available to assist with specific tasks in this project. Use `activate_skill` to load their specific workflows when needed:

*   **`israeli-crypto-tax`**: Provides expert guidance for Israeli Tax Authority (ITA) cryptocurrency reporting rules and FIFO matching algorithms. *(Highly relevant for building and auditing the core tax calculation engine `tax_engine.py` and financial data schemas).*
*   **`ccxt-python`**: Covers cryptocurrency exchange library for Python. *(Highly relevant for backend features dealing with Kraken, Binance, and other exchange API integrations).*
*   **`fastapi-async-patterns`**: Guides on FastAPI async patterns. *(Relevant for backend development, managing concurrent requests, and API optimization).*
*   **`shadcn-ui`**: Expert guidance for integrating and building applications with shadcn/ui components. *(Relevant for frontend UI development and customization).*
*   **`tanstack-query-best-practices`**: Best practices for React Query. *(Relevant for managing data fetching and server state on the Next.js frontend).*
*   **`typescript-advanced-types`**: Master TypeScript's advanced type system. *(Relevant for ensuring robust compile-time type safety in the React frontend).*
*   **`spot`**: Binance Spot API guide. *(Relevant for specialized Binance integration tasks).*
*   **`crypto-report`**: Analyzes cryptocurrency projects. *(Broad domain relevance for generating insights on crypto assets).*
