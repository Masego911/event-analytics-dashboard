import { createContext, useContext, useEffect, useMemo, useState } from 'react'

const NavigationContext = createContext(null)

function readLocation() {
  return {
    pathname: window.location.pathname,
    search: window.location.search,
    key: `${window.location.pathname}${window.location.search}`,
  }
}

export function NavigationProvider({ children }) {
  const [location, setLocation] = useState(readLocation)

  useEffect(() => {
    const handlePopState = () => setLocation(readLocation())
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  const value = useMemo(() => ({
    location,
    navigate(to, { replace = false } = {}) {
      if (replace) window.history.replaceState({}, '', to)
      else window.history.pushState({}, '', to)
      setLocation(readLocation())
      window.scrollTo({ top: 0, behavior: 'instant' })
    },
  }), [location])

  return <NavigationContext.Provider value={value}>{children}</NavigationContext.Provider>
}

export function useNavigation() {
  const context = useContext(NavigationContext)
  if (!context) throw new Error('useNavigation must be used inside NavigationProvider')
  return context
}

export function Link({ to, onClick, children, ...props }) {
  const { navigate } = useNavigation()

  function handleClick(event) {
    onClick?.(event)
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    event.preventDefault()
    navigate(to)
  }

  return <a href={to} onClick={handleClick} {...props}>{children}</a>
}
