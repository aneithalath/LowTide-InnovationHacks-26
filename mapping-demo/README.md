# Mapping Demo

This project is a React application bootstrapped with Vite, using TypeScript and Tailwind CSS for styling.

## Getting Started

### Development

1. Install dependencies:
   ```sh
   npm install
   ```
2. Start the development server:
   ```sh
   npm run dev
   ```

### Build

To build the project for production:
```sh
npm run build
```

### Preview Production Build

To preview the production build locally:
```sh
npm run preview
```

## Features
- React 18
- Vite for fast development
- TypeScript for type safety
- Tailwind CSS for utility-first styling
- OpenSky aircraft map layer with category-based icons

## OpenSky Credentials

The aircraft layer uses a Vite server middleware that performs the OpenSky OAuth2 client credentials flow and forwards requests with a Bearer token.

Aircraft responses are cached for 2 minutes (via `/api/aircraft-cache`, `/aircraft-cache.json`, and localStorage) and reused when the viewport overlap is high, reducing unnecessary API calls during small map movements.

Set these in `.env.local`:

```sh
OPENSKY_CLIENT_ID=your-client-id
OPENSKY_CLIENT_SECRET=your-client-secret
```

Then restart `npm run dev`.

## Tailwind CSS Setup
Tailwind CSS is already configured. You can use its utility classes in your components. The main CSS file is `src/index.css`.

## Customization
Edit `src/App.tsx` to start building your app.

## Project Structure
- `src/` - Source files including React components
- `public/` - Static assets
- `src/index.css` - Global styles with Tailwind CSS
- `tailwind.config.js` - Tailwind CSS configuration
- `vite.config.ts` - Vite configuration

## Contributing
Feel free to modify and extend this project as needed for your mapping demo.

---

This project demonstrates a modern React development setup with Vite, TypeScript, and Tailwind CSS.
