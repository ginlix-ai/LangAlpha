# SuperRich Frontend

A modern React application built with Vite, featuring a multi-page dashboard with sidebar navigation.

## Features

- 🎨 **Modern UI**: Built with Ant Design components
- 🧭 **Routing**: React Router v6 for seamless navigation
- 📱 **Responsive Layout**: Fixed sidebar with full-width content area
- 🎯 **Four Main Pages**:
  - Dashboard
  - Chat Agent
  - Personal Home
  - Trading Center

## Tech Stack

- **React 18.2** - UI library
- **Vite 5.0** - Build tool and dev server
- **React Router DOM 6.21** - Client-side routing
- **Ant Design 5.12** - UI component library
- **ESLint** - Code linting

## Project Structure

```
src/
├── components/
│   ├── Sidebar/          # Navigation sidebar component
│   └── Main/             # Routing component
├── pages/
│   ├── Dashboard/        # Dashboard page
│   ├── ChatAgent/        # Chat Agent page
│   ├── PersonalHome/     # Personal Home page
│   └── TradingCenter/    # Trading Center page
├── App.jsx               # Main app component
└── main.jsx              # Application entry point
```

## Getting Started

### Prerequisites

- Node.js (v16 or higher)
- npm or yarn

### Installation

1. Clone the repository:
```bash
git clone https://github.com/leo-Zhizhu/SuperRichFrontend.git
cd SuperRichFrontend
```

2. Install dependencies:
```bash
npm install
```

### Development

Run the development server:

```bash
npm run dev
```

The application will be available at `http://localhost:5173`

### Build

Build for production:

```bash
npm run build
```

The production build will be in the `dist` directory.

### Preview

Preview the production build:

```bash
npm run preview
```

## Routes

- `/` - Redirects to `/dashboard`
- `/dashboard` - Dashboard page
- `/chat` - Chat Agent page
- `/home` - Personal Home page
- `/trading` - Trading Center page

## Navigation

The application features a fixed sidebar on the left side with navigation icons for each page. Clicking on any icon will navigate to the corresponding page while maintaining the sidebar visibility.

## License

This project is part of a hackathon submission.
